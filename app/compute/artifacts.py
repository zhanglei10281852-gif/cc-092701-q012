from __future__ import annotations

import hashlib
import os
import shutil
from pathlib import Path

from app.core.errors import ConflictError, NotFoundError, ValidationError

_CHUNK = 1024 * 1024
_STAGING = "staging"
_BLOBS = "blobs"


class ArtifactStore:
    """成果文件物理存储：受控暂存区 + 按 sha256 内容寻址的 blob 区。

    暂存对象名与 blob 对象名都由服务端生成，调用方给出的相对路径只作为
    清单里的逻辑标签参与核对，永远不会被拼进文件系统路径。
    """

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.staging_dir = self.root / _STAGING
        self.blobs_dir = self.root / _BLOBS

    def ensure_dirs(self) -> None:
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        self.blobs_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _safe_name(name: str) -> str:
        if not name or "/" in name or "\\" in name or name in {".", ".."}:
            raise ValidationError("非法的存储对象名")
        return name

    def staging_path(self, upload_id: str) -> Path:
        return self.staging_dir / self._safe_name(upload_id)

    def blob_relpath(self, sha256: str) -> str:
        digest = self._safe_name(sha256)
        return f"{_BLOBS}/{digest[:2]}/{digest}"

    def absolute(self, relpath: str) -> Path:
        candidate = (self.root / relpath).resolve()
        root = self.root.resolve()
        if root not in candidate.parents and candidate != root:
            raise ValidationError("存储路径越界")
        return candidate

    def save_staging(self, upload_id: str, chunks) -> tuple[int, str]:
        """把上传字节流原子地写入暂存区，返回 (大小, sha256)。"""
        self.ensure_dirs()
        target = self.staging_path(upload_id)
        if target.exists():
            raise ConflictError("该上传标识已有暂存内容")
        temporary = target.with_suffix(target.suffix + ".tmp")
        hasher = hashlib.sha256()
        size = 0
        try:
            with temporary.open("wb") as handle:
                for chunk in chunks:
                    if not chunk:
                        continue
                    handle.write(chunk)
                    hasher.update(chunk)
                    size += len(chunk)
            os.replace(temporary, target)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
        return size, hasher.hexdigest()

    async def save_staging_stream(self, upload_id: str, async_chunks) -> tuple[int, str]:
        """把异步上传字节流写入暂存区，返回 (大小, sha256)。"""
        self.ensure_dirs()
        target = self.staging_path(upload_id)
        if target.exists():
            raise ConflictError("该上传标识已有暂存内容")
        temporary = target.with_suffix(target.suffix + ".tmp")
        hasher = hashlib.sha256()
        size = 0
        try:
            with temporary.open("wb") as handle:
                async for chunk in async_chunks:
                    if not chunk:
                        continue
                    handle.write(chunk)
                    hasher.update(chunk)
                    size += len(chunk)
            os.replace(temporary, target)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
        return size, hasher.hexdigest()

    def inspect_staging(self, upload_id: str) -> Path:
        path = self.staging_path(upload_id)
        if not path.exists():
            raise NotFoundError("暂存文件不存在或已过期")
        return path

    def commit_verified(self, upload_id: str, sha256: str, size_bytes: int) -> str:
        """把已核对的暂存内容提交为不可变 blob（不再重复摘要计算）。

        调用方必须已通过 hash_file 复算并比对通过；这里仅做存在性与
        大小的廉价防护。相同摘要复用既有 blob。返回相对路径。
        """
        self.ensure_dirs()
        source = self.staging_path(upload_id)
        if not source.exists():
            raise NotFoundError("暂存文件不存在或已过期")
        if source.stat().st_size != size_bytes:
            raise ConflictError("内容寻址存储提交时大小发生变化")
        relpath = self.blob_relpath(sha256)
        destination = self.absolute(relpath)
        if destination.exists():
            if destination.stat().st_size != size_bytes:
                raise ConflictError("内容寻址存储命中了大小不一致的对象")
            return relpath
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_suffix(".tmp")
        try:
            # 优先硬链接（同一文件系统、零拷贝），失败再回退到复制。
            try:
                os.link(source, temporary)
            except OSError:
                shutil.copyfile(source, temporary)
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)
        return relpath

    @staticmethod
    def hash_file(path: Path) -> tuple[int, str]:
        hasher = hashlib.sha256()
        size = 0
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(_CHUNK), b""):
                hasher.update(chunk)
                size += len(chunk)
        return size, hasher.hexdigest()

    def verify_blob(self, relpath: str, sha256: str, size_bytes: int) -> Path:
        path = self.absolute(relpath)
        if not path.exists():
            raise NotFoundError("成果文件内容缺失")
        if path.stat().st_size != size_bytes:
            raise ConflictError("成果文件大小与登记不一致")
        hasher = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(_CHUNK), b""):
                hasher.update(chunk)
        if hasher.hexdigest() != sha256:
            raise ConflictError("成果文件摘要与登记不一致")
        return path

    def delete_staging(self, upload_id: str) -> bool:
        path = self.staging_path(upload_id)
        if path.exists():
            path.unlink()
            return True
        return False

    def delete_blob(self, relpath: str) -> bool:
        path = self.absolute(relpath)
        if path.exists():
            path.unlink()
            return True
        return False
