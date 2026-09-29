from __future__ import annotations

import hashlib
import os
from pathlib import Path

from app.core.errors import ConflictError, NotFoundError, ValidationError


class ArtifactBlobStore:
    """内容寻址的成果文件存储。

    文件以 sha256 摘要作为键，按前两位分目录存放。相同内容只落一份物理文件，
    清理时由数据库侧的引用计数决定是否真正删除。
    """

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def _relpath_for(self, digest: str) -> str:
        return f"{digest[:2]}/{digest[2:]}"

    def path_for(self, digest: str) -> Path:
        return self.root / self._relpath_for(digest)

    def put_bytes(self, data: bytes, *, declared_sha256: str = "", declared_size: int | None = None) -> tuple[str, int, str]:
        """写入一段内容并核对工作者声明的摘要与大小，返回 (实际摘要, 实际大小, 相对路径)。"""
        actual_sha = hashlib.sha256(data).hexdigest()
        if declared_sha256 and declared_sha256.lower() != actual_sha:
            raise ConflictError(
                "成果文件摘要与实际内容不符，任务不得转为成功",
                context={"declared_sha256": declared_sha256.lower(), "actual_sha256": actual_sha},
            )
        if declared_size is not None and declared_size != len(data):
            raise ConflictError(
                "成果文件大小与实际内容不符，任务不得转为成功",
                context={"declared_size": declared_size, "actual_size": len(data)},
            )
        relpath = self._relpath_for(actual_sha)
        target = self.root / relpath
        if not target.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = target.with_name(f".{target.name}.tmp")
            temporary.write_bytes(data)
            os.replace(temporary, target)
        return actual_sha, len(data), relpath

    def read(self, storage_relpath: str) -> bytes:
        path = (self.root / storage_relpath).resolve()
        if not self._is_within_root(path):
            raise ValidationError("成果文件存储路径不合法")
        if not path.is_file():
            raise NotFoundError("成果文件物理内容已不存在")
        return path.read_bytes()

    def delete(self, storage_relpath: str) -> bool:
        path = (self.root / storage_relpath).resolve()
        if not self._is_within_root(path) or not path.is_file():
            return False
        path.unlink()
        parent = path.parent
        if parent != self.root.resolve():
            try:
                parent.rmdir()
            except (OSError, PermissionError):
                pass
        return True

    def _is_within_root(self, path: Path) -> bool:
        root = self.root.resolve()
        try:
            path.relative_to(root)
        except ValueError:
            return False
        return True
