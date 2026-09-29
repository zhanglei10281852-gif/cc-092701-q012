from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from app.compute.artifacts import ArtifactStore
from app.compute.repository import ComputeRepository
from app.core.clock import Clock, SystemClock, from_storage, to_storage
from app.core.config import Settings
from app.core.errors import ConflictError, NotFoundError, PermissionDeniedError, ValidationError
from app.database import get_connection, transaction

# 成果版本生命周期状态
STATE_CANDIDATE = "candidate"
STATE_PUBLISHED = "published"
STATE_WITHDRAWN = "withdrawn"


def digest(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


class ComputeOperationsService:
    """管理计算模板、配额、任务租约、结果版本和人工干预。"""

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None, settings: Settings | None = None, store: ArtifactStore | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        self.settings = settings or Settings.load()
        self.store = store or ArtifactStore(self.settings.artifact_store_path)
        self.repository = ComputeRepository(self.connection)

    def list_templates(self) -> list[dict[str, Any]]:
        return self.repository.active_templates()

    def create_template(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        self._validate_schema(payload["parameter_schema"], payload["default_parameters"])
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            if repository.template_by_code(payload["code"]):
                raise ConflictError("参数模板编码已存在")
            return repository.create_template(
                code=payload["code"], name=payload["name"], algorithm=payload["algorithm"],
                parameter_schema=payload["parameter_schema"], defaults=payload["default_parameters"],
                max_runtime_seconds=payload["max_runtime_seconds"], max_attempts=payload["max_attempts"],
                created_by=actor, now=now,
            )

    def set_quota(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            return ComputeRepository(connection).upsert_quota(actor=actor, now=now, **payload)

    def submit(self, payload: dict[str, Any]) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            template = repository.template_by_code(payload["template_code"])
            if template is None or not template["active"]:
                raise NotFoundError("参数模板不存在或已经停用")
            parameters = self._validate_parameters(template, payload["parameters"])
            existing = repository.task_by_idempotency(payload["requested_by"], payload["idempotency_key"])
            parameter_digest = digest(parameters)
            if existing is not None:
                if existing["parameter_digest"] != parameter_digest:
                    raise ConflictError("同一幂等键对应了不同的计算参数")
                return dict(repository.task_by_id(existing["id"]))
            self._check_quota(repository, payload["requested_by"], now_value)
            return repository.create_task(
                template_id=template["id"], project_code=payload["project_code"],
                requested_by=payload["requested_by"], parameters=parameters,
                parameter_digest=parameter_digest, priority=payload["priority"],
                idempotency_key=payload["idempotency_key"], max_attempts=template["max_attempts"], now=now,
            )

    def list_tasks(self, *, status: str | None = None, project_code: str | None = None, requested_by: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return self.repository.list_tasks(status=status, project_code=project_code, requested_by=requested_by, limit=max(1, min(limit, 500)))

    def get_task(self, task_id: int) -> dict[str, Any]:
        row = self.repository.task_by_id(task_id)
        if row is None:
            raise NotFoundError("计算任务不存在")
        result = dict(row)
        result["results"] = self.repository.result_versions(task_id)
        result["artifacts"] = self.repository.result_artifacts(task_id)
        result["interventions"] = self.repository.interventions(task_id)
        return result

    def claim(self, worker_id: str, capabilities: list[str], lease_seconds: int) -> dict[str, Any] | None:
        now_value = self.clock.now()
        now = to_storage(now_value)
        lease_until = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            candidate = repository.queued_candidate(capabilities, now)
            if candidate is None:
                return None
            cursor = connection.execute(
                "UPDATE compute_tasks SET status='running',attempt_count=attempt_count+1,lease_owner=?,lease_expires_at=?,started_at=COALESCE(started_at,?),updated_at=?,version=version+1 WHERE id=? AND status='queued'",
                (worker_id, lease_until, now, now, candidate["id"]),
            )
            if cursor.rowcount != 1:
                return None
            return dict(repository.task_by_id(candidate["id"]))

    def heartbeat(self, task_id: int, worker_id: str, lease_seconds: int) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        expires = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            cursor = connection.execute(
                "UPDATE compute_tasks SET lease_expires_at=?,updated_at=?,version=version+1 WHERE id=? AND status='running' AND lease_owner=?",
                (expires, now, task_id, worker_id),
            )
            if cursor.rowcount != 1:
                raise ConflictError("任务未由当前工作者持有")
            return dict(ComputeRepository(connection).task_by_id(task_id))

    def complete(self, task_id: int, worker_id: str, result: dict[str, Any], metrics: dict[str, Any], artifacts: list[dict[str, Any]] | None = None, receipt_key: str | None = None) -> dict[str, Any]:
        artifacts = artifacts or []
        now_value = self.clock.now()
        now = to_storage(now_value)
        candidate_retention = to_storage(now_value + timedelta(days=self.settings.artifact_candidate_retention_days))
        staging_files: list[str] = []
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")

            # 重复回执只能产生一份清单：命中既有回执即幂等返回对应版本（即便任务已成功）。
            if receipt_key:
                existing_receipt = repository.receipt(task_id, receipt_key)
                if existing_receipt is not None:
                    return self._task_with_artifacts(repository, task_id)

            if task["status"] != "running" or task["lease_owner"] != worker_id:
                raise ConflictError("任务未由当前工作者持有")

            version = int(connection.execute("SELECT COALESCE(MAX(version),0)+1 FROM compute_results WHERE task_id=?", (task_id,)).fetchone()[0])
            repository.insert_result(
                task_id=task_id, version=version, result=result, metrics=metrics,
                result_digest=digest({"result": result, "metrics": metrics}), created_by=worker_id,
                now=now, lifecycle_state=STATE_CANDIDATE, retention_until=candidate_retention,
            )

            # 阶段一：先核对清单（只读）。任何一项不过都抛错回滚，任务不转成功。
            verified: list[dict[str, Any]] = []
            seen_filenames: set[str] = set()
            seen_uploads: set[str] = set()
            for item in artifacts:
                filename = item["filename"]
                if filename in seen_filenames:
                    raise ValidationError("清单中存在重名成果文件", context={"filename": filename})
                seen_filenames.add(filename)
                declared_path = self._safe_relative_path(item["path"])
                upload_id = item["upload_id"]
                if upload_id in seen_uploads:
                    raise ValidationError("清单中存在重复的上传标识", context={"upload_id": upload_id})
                seen_uploads.add(upload_id)
                staging = repository.staging_by_upload(upload_id)
                if staging is None:
                    raise NotFoundError(f"成果文件未上传或暂存已过期：{filename}", context={"upload_id": upload_id})
                if staging["uploaded_by"] != worker_id:
                    raise PermissionDeniedError("只能引用本工作者上传的成果文件")
                if staging["storage_relpath"] != declared_path:
                    raise ValidationError("成果文件清单路径与上传登记路径不一致", context={"declared_path": declared_path, "stored_path": staging["storage_relpath"]})
                if staging["filename"] != filename or staging["purpose"] != item["purpose"]:
                    raise ValidationError("成果文件清单与上传登记不一致", context={"filename": filename})
                declared_size = int(item["size_bytes"])
                declared_sha = item["sha256"]
                # 独立复算物理内容的大小与摘要；不符则任务不能转为成功。
                actual_size, actual_sha = self.store.hash_file(self.store.inspect_staging(upload_id))
                if actual_size != declared_size or actual_sha != declared_sha:
                    raise ValidationError(
                        "成果文件清单核对失败：大小或摘要与实际内容不符",
                        context={"filename": filename, "declared_size": declared_size, "actual_size": actual_size, "declared_sha256": declared_sha, "actual_sha256": actual_sha},
                    )
                verified.append({"item": item, "upload_id": upload_id, "path": declared_path, "size": actual_size, "sha": actual_sha})

            # 阶段二：核对全部通过后才提升 blob 并落库清单。
            for entry in verified:
                item = entry["item"]
                relpath = self.store.commit_verified(entry["upload_id"], entry["sha"], entry["size"])
                blob = repository.blob_by_digest(entry["sha"])
                blob_id = int(blob["id"]) if blob is not None else repository.insert_blob(sha256=entry["sha"], size_bytes=entry["size"], storage_relpath=relpath, now=now)
                repository.insert_result_artifact(
                    task_id=task_id, result_version=version, blob_id=blob_id,
                    declared_path=entry["path"], filename=item["filename"], purpose=item["purpose"],
                    declared_size=entry["size"], size_bytes=entry["size"],
                    sha256=entry["sha"], state=STATE_CANDIDATE, retention_until=candidate_retention,
                    created_by=worker_id, now=now,
                )
                repository.delete_staging(entry["upload_id"])
                staging_files.append(entry["upload_id"])

            if receipt_key:
                repository.add_receipt(task_id=task_id, receipt_key=receipt_key, result_version=version, created_by=worker_id, now=now)

            connection.execute(
                "UPDATE compute_tasks SET status='succeeded',current_result_version=?,lease_owner='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (version, now, now, task_id),
            )
            task_view = self._task_with_artifacts(repository, task_id)
        # 事务提交后再清理暂存物理文件（blob 已独立硬链/复制）。
        for upload_id in staging_files:
            self.store.delete_staging(upload_id)
        return task_view

    @staticmethod
    def _task_with_artifacts(repository: ComputeRepository, task_id: int) -> dict[str, Any]:
        task = repository.task_by_id(task_id)
        result = dict(task)
        result["results"] = repository.result_versions(task_id)
        result["artifacts"] = repository.result_artifacts(task_id)
        result["interventions"] = repository.interventions(task_id)
        return result

    # ----- 成果文件：上传登记 / 授权 / 下载 -----

    async def register_artifact_upload(self, upload_id: str, worker_id: str, filename: str, purpose: str, body) -> dict[str, Any]:
        safe_filename = self._safe_filename(filename)
        now_value = self.clock.now()
        now = to_storage(now_value)
        expires = to_storage(now_value + timedelta(hours=self.settings.artifact_temp_retention_hours))
        with transaction(immediate=True) as connection:
            if ComputeRepository(connection).staging_by_upload(upload_id) is not None:
                raise ConflictError("该上传标识已登记")
        size_bytes, sha256 = await self.store.save_staging_stream(upload_id, body)
        relpath = f"staging/{upload_id}"
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            if repository.staging_by_upload(upload_id) is not None:
                self.store.delete_staging(upload_id)
                raise ConflictError("该上传标识已登记")
            repository.create_staging(
                upload_id=upload_id, blob_id=None, storage_relpath=relpath, filename=safe_filename,
                purpose=purpose, size_bytes=size_bytes, sha256=sha256, uploaded_by=worker_id,
                now=now, expires_at=expires,
            )
        return {
            "upload_id": upload_id, "path": relpath, "filename": safe_filename, "purpose": purpose,
            "size_bytes": size_bytes, "sha256": sha256, "expires_at": expires,
        }

    @staticmethod
    def _safe_filename(filename: str) -> str:
        cleaned = filename.replace("\\", "/").split("/")[-1].strip()
        if not cleaned or cleaned in {".", ".."} or any(ch in cleaned for ch in "\x00\r\n"):
            raise ValidationError("非法的成果文件名")
        return cleaned[:200]

    @staticmethod
    def _safe_relative_path(value: str) -> str:
        cleaned = (value or "").replace("\\", "/").strip().lstrip("/")
        if not cleaned or any(ch in cleaned for ch in "\x00"):
            raise ValidationError("成果文件路径不能为空")
        parts = [part for part in cleaned.split("/") if part not in {"", "."}]
        if any(part == ".." for part in parts):
            raise ValidationError("成果文件路径不能包含上级目录")
        if ":" in parts[-1] or len(cleaned) > 300:
            raise ValidationError("成果文件路径不合法")
        return "/".join(parts)

    def grant_project_member(self, payload: dict[str, Any]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            return ComputeRepository(connection).grant_project_member(
                project_code=payload["project_code"], member=payload["member"], role=payload["role"],
                granted_by=payload["granted_by"], now=now,
            )

    def _retention_for(self, state: str, now_value: datetime) -> str:
        if state == STATE_PUBLISHED:
            # 公开成绩引用的成果长期保留；保留期之外仍受引用保护。
            return to_storage(now_value + timedelta(days=self.settings.artifact_published_retention_days))
        if state == STATE_WITHDRAWN:
            return to_storage(now_value + timedelta(days=self.settings.artifact_withdrawn_retention_days))
        return to_storage(now_value + timedelta(days=self.settings.artifact_candidate_retention_days))

    def publish_result(self, task_id: int, version: int, actor: str, reason: str) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        retention = self._retention_for(STATE_PUBLISHED, now_value)
        withdrawn_retention = self._retention_for(STATE_WITHDRAWN, now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            result = repository.result_version(task_id, version)
            if result is None:
                raise NotFoundError("成绩版本不存在")
            task = repository.task_by_id(task_id)
            previous_published = task["published_result_version"]
            before = dict(result)
            repository.set_result_lifecycle(
                task_id=task_id, version=version, state=STATE_PUBLISHED, retention_until=retention,
                published_at=now, withdrawn_at=None, withdraw_reason="",
            )
            repository.set_artifacts_lifecycle(
                task_id=task_id, version=version, state=STATE_PUBLISHED, retention_until=retention,
                published_at=now, withdrawn_at=None, now=now,
            )
            # 公开成绩当前引用指向新版本；这是受引用保护的唯一依据，不随候选提交改变。
            connection.execute(
                "UPDATE compute_tasks SET published_result_version=?,updated_at=?,version=version+1 WHERE id=?",
                (version, now, task_id),
            )
            # 发布新版本会把旧的已发布版本级联为“被替代”，进入撤回留存期（不再被公开成绩引用）。
            if previous_published is not None and int(previous_published) != version:
                old = repository.result_version(task_id, int(previous_published))
                if old is not None and old["lifecycle_state"] == STATE_PUBLISHED:
                    supersede_reason = f"成绩版本 {version} 发布，版本 {previous_published} 被替代"
                    repository.set_result_lifecycle(
                        task_id=task_id, version=int(previous_published), state=STATE_WITHDRAWN,
                        retention_until=withdrawn_retention, published_at=None, withdrawn_at=now,
                        withdraw_reason=supersede_reason,
                    )
                    repository.set_artifacts_lifecycle(
                        task_id=task_id, version=int(previous_published), state=STATE_WITHDRAWN,
                        retention_until=withdrawn_retention, published_at=None, withdrawn_at=now, now=now,
                    )
                    repository.add_intervention(task_id=task_id, actor=actor, action="supersede_result", reason=supersede_reason, before=dict(old), after=dict(repository.result_version(task_id, int(previous_published))), batch_key="", now=now)
            after = dict(repository.result_version(task_id, version))
            if before.get("lifecycle_state") != STATE_PUBLISHED:
                repository.add_intervention(task_id=task_id, actor=actor, action="publish_result", reason=reason or "发布成绩版本", before=before, after=after, batch_key="", now=now)
            return self._version_detail(repository, task_id, version)

    def withdraw_result(self, task_id: int, version: int, actor: str, reason: str) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        retention = self._retention_for(STATE_WITHDRAWN, now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            result = repository.result_version(task_id, version)
            if result is None:
                raise NotFoundError("成绩版本不存在")
            if result["lifecycle_state"] != STATE_PUBLISHED:
                raise ConflictError("只有已发布的成绩版本可以撤回")
            task = repository.task_by_id(task_id)
            before = dict(result)
            repository.set_result_lifecycle(
                task_id=task_id, version=version, state=STATE_WITHDRAWN, retention_until=retention,
                published_at=None, withdrawn_at=now, withdraw_reason=reason,
            )
            repository.set_artifacts_lifecycle(
                task_id=task_id, version=version, state=STATE_WITHDRAWN, retention_until=retention,
                published_at=None, withdrawn_at=now, now=now,
            )
            # 撤回即解除公开成绩引用；仅当该版本正是当前公开版本时清除。
            if task["published_result_version"] == version:
                connection.execute(
                    "UPDATE compute_tasks SET published_result_version=NULL,updated_at=?,version=version+1 WHERE id=?",
                    (now, task_id),
                )
            after = dict(repository.result_version(task_id, version))
            repository.add_intervention(task_id=task_id, actor=actor, action="withdraw_result", reason=reason, before=before, after=after, batch_key="", now=now)
            return self._version_detail(repository, task_id, version)

    def _version_detail(self, repository: ComputeRepository, task_id: int, version: int) -> dict[str, Any]:
        result = repository.result_version(task_id, version)
        detail = dict(result)
        detail["artifacts"] = repository.result_artifacts(task_id, version)
        return detail

    def _authorize_artifact(self, repository: ComputeRepository, task: sqlite3.Row, artifact: sqlite3.Row, requested_by: str, now_value: datetime) -> tuple[bool, list[str]]:
        reasons: list[str] = []
        allowed = True

        is_owner = task["requested_by"] == requested_by
        member = repository.project_member(task["project_code"], requested_by)
        is_teacher = member is not None and member["role"] == "teacher"
        is_admin = member is not None and member["role"] == "admin"
        if is_owner:
            reasons.append("作业提交者本人")
        if is_teacher:
            reasons.append(f"项目 {task['project_code']} 的任课教师")
        if is_admin:
            reasons.append(f"项目 {task['project_code']} 的管理员")
        if not (is_owner or is_teacher or is_admin):
            allowed = False
            reasons.append("不是作业提交者，也不是该项目的教师或管理员，无权访问")

        state = artifact["state"]
        retention_until = artifact["retention_until"]
        within_retention = bool(retention_until) and from_storage(retention_until) is not None and from_storage(retention_until) >= now_value
        referenced = self._is_public_current(repository, task, artifact)

        if state == STATE_PUBLISHED:
            if is_owner and not (is_teacher or is_admin):
                allowed = False
                reasons.append("成果已随公开成绩发布，提交者本人不能直接下载，须由任课教师访问")
            elif referenced:
                reasons.append("仍被公开成绩引用，处于强制保留期")
            elif within_retention:
                reasons.append("已发布且在保留期内")
            else:
                allowed = False
                reasons.append("已发布但已超出保留期且无公开成绩引用")
        elif state == STATE_CANDIDATE:
            if is_owner:
                allowed = False
                reasons.append("候选版本尚未发布，提交者不能下载")
            elif within_retention:
                reasons.append("候选版本在保留期内，教师可预览")
            else:
                allowed = False
                reasons.append("候选版本已超出保留期")
        elif state == STATE_WITHDRAWN:
            if is_admin:
                if within_retention or referenced:
                    reasons.append("撤回版本在留存/引用保护期内，管理员可审计访问")
                else:
                    allowed = False
                    reasons.append("撤回版本已超出留存期")
            else:
                allowed = False
                reasons.append("版本已撤回，教师与提交者不可访问，仅管理员可在留存期内审计")

        if referenced and not allowed:
            # 被引用内容必须保留，但保留不等于对无权限者开放下载。
            reasons.append("注意：该文件被公开成绩引用而保留，但当前身份仍无权下载")
        return allowed, reasons

    @staticmethod
    def _is_public_current(repository: ComputeRepository, task: sqlite3.Row, artifact: sqlite3.Row) -> bool:
        if artifact["state"] != STATE_PUBLISHED:
            return False
        if task is None:
            task = repository.task_by_id(artifact["task_id"])
        return task["published_result_version"] == artifact["result_version"]

    def artifact_access(self, task_id: int, version: int, filename: str, requested_by: str) -> dict[str, Any]:
        now_value = self.clock.now()
        repository = self.repository
        task = repository.task_by_id(task_id)
        if task is None:
            raise NotFoundError("计算任务不存在")
        artifact = repository.artifact_by_filename(task_id, version, filename)
        if artifact is None:
            raise NotFoundError("成果文件不存在")
        allowed, reasons = self._authorize_artifact(repository, task, artifact, requested_by, now_value)
        return self._access_view(task, artifact, requested_by, allowed, reasons)

    def artifact_access_by_id(self, artifact_id: int, requested_by: str) -> dict[str, Any]:
        now_value = self.clock.now()
        repository = self.repository
        artifact = repository.artifact_by_id(artifact_id)
        if artifact is None:
            raise NotFoundError("成果文件不存在")
        task = repository.task_by_id(artifact["task_id"])
        allowed, reasons = self._authorize_artifact(repository, task, artifact, requested_by, now_value)
        return self._access_view(task, artifact, requested_by, allowed, reasons)

    def _access_view(self, task: sqlite3.Row, artifact: sqlite3.Row, requested_by: str, allowed: bool, reasons: list[str]) -> dict[str, Any]:
        referenced = self._is_public_current(self.repository, task, artifact)
        return {
            "allowed": allowed,
            "requested_by": requested_by,
            "decision": "allow" if allowed else "deny",
            "reasons": reasons,
            "artifact": {
                "id": artifact["id"],
                "task_id": artifact["task_id"],
                "result_version": artifact["result_version"],
                "declared_path": artifact["declared_path"],
                "filename": artifact["filename"],
                "purpose": artifact["purpose"],
                "size_bytes": artifact["size_bytes"],
                "sha256": artifact["sha256"],
                "state": artifact["state"],
                "retention_until": artifact["retention_until"],
            },
            "result_version": {
                "task_id": artifact["task_id"],
                "version": artifact["result_version"],
                "state": artifact["state"],
            },
            "task": {"id": task["id"], "project_code": task["project_code"], "requested_by": task["requested_by"], "current_result_version": task["current_result_version"], "published_result_version": task["published_result_version"]},
            "referenced_by_public_grade": referenced,
        }

    def open_artifact_download(self, task_id: int, version: int, filename: str, requested_by: str):
        now_value = self.clock.now()
        repository = self.repository
        task = repository.task_by_id(task_id)
        if task is None:
            raise NotFoundError("计算任务不存在")
        artifact = repository.artifact_by_filename(task_id, version, filename)
        if artifact is None:
            raise NotFoundError("成果文件不存在")
        allowed, reasons = self._authorize_artifact(repository, task, artifact, requested_by, now_value)
        if not allowed:
            raise PermissionDeniedError("成果文件下载被拒绝", context={"reasons": reasons})
        blob = repository.blob_by_id(artifact["blob_id"])
        path = self.store.verify_blob(blob["storage_relpath"], artifact["sha256"], artifact["size_bytes"])
        return path, dict(artifact), reasons

    # ----- 生命周期清理 -----

    def cleanup_artifacts(self, actor: str = "retention-worker") -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        removed_temp: list[str] = []
        removed_candidate: list[int] = []
        removed_withdrawn: list[int] = []
        protected_published = 0
        staging_files: list[str] = []
        blob_files: list[str] = []
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)

            # 临时文件：暂存项一旦过期即可清理（不被成绩引用）。
            for staging in repository.expired_staging(now):
                repository.delete_staging(staging["upload_id"])
                staging_files.append(staging["upload_id"])
                removed_temp.append(staging["upload_id"])

            # 候选版本：超过候选保留期，且不是当前公开成绩（候选本身也不会是公开成绩）。
            for artifact in repository.expired_result_artifacts(STATE_CANDIDATE, now):
                if self._is_public_current(repository, repository.task_by_id(artifact["task_id"]), artifact):
                    continue
                blob_id = artifact["blob_id"]
                repository.delete_result_artifact(artifact["id"])
                removed_candidate.append(artifact["id"])
                self._release_blob(repository, blob_id, blob_files)

            # 撤回版本：超过撤回留存期。
            for artifact in repository.expired_result_artifacts(STATE_WITHDRAWN, now):
                task = repository.task_by_id(artifact["task_id"])
                if self._is_public_current(repository, task, artifact):
                    continue
                blob_id = artifact["blob_id"]
                repository.delete_result_artifact(artifact["id"])
                removed_withdrawn.append(artifact["id"])
                self._release_blob(repository, blob_id, blob_files)

            # 已发布：被公开成绩引用的必须保留，仅统计受保护数量，不删除。
            protected_published = int(connection.execute(
                "SELECT COUNT(*) FROM compute_result_artifacts WHERE state=?", (STATE_PUBLISHED,)
            ).fetchone()[0])

        # 事务提交后才删除物理文件，避免回滚后数据库仍指向已删文件。
        for upload_id in staging_files:
            self.store.delete_staging(upload_id)
        for relpath in blob_files:
            self.store.delete_blob(relpath)

        return {
            "finished_at": now,
            "removed_temp_uploads": removed_temp,
            "removed_candidate_artifacts": removed_candidate,
            "removed_withdrawn_artifacts": removed_withdrawn,
            "protected_published_artifacts": protected_published,
            "removed_unreferenced_blobs": blob_files,
        }

    def _release_blob(self, repository: ComputeRepository, blob_id: int, blob_files: list[str]) -> None:
        """事务内：若 blob 已无任何清单引用，删除其登记行并记下物理路径，待提交后再删文件。"""
        if repository.blob_is_referenced(blob_id):
            return
        blob = repository.blob_by_id(blob_id)
        if blob is None:
            return
        relpath = blob["storage_relpath"]
        repository.connection.execute("DELETE FROM compute_artifact_blobs WHERE id=?", (blob_id,))
        blob_files.append(relpath)

    def fail(self, task_id: int, worker_id: str, error_code: str, message: str, retryable: bool) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            if task["status"] != "running" or task["lease_owner"] != worker_id:
                raise ConflictError("任务未由当前工作者持有")
            can_retry = retryable and int(task["attempt_count"]) < int(task["max_attempts"])
            status = "queued" if can_retry else "failed"
            delay = min(300, 2 ** max(0, int(task["attempt_count"]) - 1)) if can_retry else 0
            available = to_storage(now_value + timedelta(seconds=delay))
            connection.execute(
                "UPDATE compute_tasks SET status=?,available_at=?,lease_owner='',lease_expires_at='',last_error_code=?,last_error_message=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (status, available, error_code, message[:2000], None if can_retry else now, now, task_id),
            )
            return dict(repository.task_by_id(task_id))

    def cancel(self, task_id: int, actor: str, reason: str, batch_key: str = "") -> dict[str, Any]:
        return self._intervene(task_id, actor, reason, "cancel", batch_key, self._cancel_mutation)

    def retry(self, task_id: int, actor: str, reason: str, priority: int | None = None, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
            if task["status"] not in {"failed", "cancelled", "succeeded"}:
                raise ConflictError("只有失败、已取消或已成功（复算）任务可以人工重试")
            chosen = task["priority"] if priority is None else priority
            # 复算已成功任务时不清空 published_result_version：公开成绩引用在新版本发布前保持有效。
            connection.execute("UPDATE compute_tasks SET status='queued',priority=?,available_at=?,lease_owner='',lease_expires_at='',finished_at=NULL,updated_at=?,version=version+1 WHERE id=?", (chosen, now, now, task["id"]))
        return self._intervene(task_id, actor, reason, "retry", batch_key, mutate)

    def set_priority(self, task_id: int, actor: str, reason: str, priority: int, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
            if task["status"] not in {"queued", "running"}:
                raise ConflictError("只有排队或运行中的任务可以调整优先级")
            connection.execute("UPDATE compute_tasks SET priority=?,updated_at=?,version=version+1 WHERE id=?", (priority, now, task["id"]))
        return self._intervene(task_id, actor, reason, "priority", batch_key, mutate)

    def batch_operation(self, payload: dict[str, Any]) -> dict[str, Any]:
        batch_key = digest({"actor": payload["actor"], "task_ids": payload["task_ids"], "operation": payload["operation"], "reason": payload["reason"]})
        succeeded: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        for task_id in list(dict.fromkeys(payload["task_ids"])):
            try:
                if payload["operation"] == "cancel":
                    value = self.cancel(task_id, payload["actor"], payload["reason"], batch_key)
                elif payload["operation"] == "retry":
                    value = self.retry(task_id, payload["actor"], payload["reason"], payload.get("priority"), batch_key)
                else:
                    value = self.set_priority(task_id, payload["actor"], payload["reason"], int(payload["priority"]), batch_key)
                succeeded.append({"task_id": task_id, "status": value["status"], "version": value["version"]})
            except (ConflictError, NotFoundError) as exc:
                failed.append({"task_id": task_id, "code": exc.code, "message": exc.message})
        return {"batch_key": batch_key, "succeeded": succeeded, "failed": failed}

    def recover_expired(self, actor: str = "recovery-worker") -> dict[str, Any]:
        now = to_storage(self.clock.now())
        recovered: list[int] = []
        exhausted: list[int] = []
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            rows = connection.execute("SELECT * FROM compute_tasks WHERE status='running' AND lease_expires_at<>'' AND lease_expires_at<? ORDER BY id", (now,)).fetchall()
            for task in rows:
                before = dict(task)
                if int(task["attempt_count"]) < int(task["max_attempts"]):
                    status, finished_at = "queued", None
                    recovered.append(int(task["id"]))
                else:
                    status, finished_at = "failed", now
                    exhausted.append(int(task["id"]))
                connection.execute(
                    "UPDATE compute_tasks SET status=?,lease_owner='',lease_expires_at='',available_at=?,last_error_code='lease_expired',last_error_message='工作者租约已过期',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (status, now, finished_at, now, task["id"]),
                )
                after = dict(repository.task_by_id(task["id"]))
                repository.add_intervention(task_id=task["id"], actor=actor, action="lease_recovery", reason="租约过期自动恢复", before=before, after=after, batch_key="", now=now)
        return {"recovered": recovered, "exhausted": exhausted}

    def summary(self) -> dict[str, Any]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM compute_tasks GROUP BY status ORDER BY status").fetchall()
        oldest = self.connection.execute("SELECT MIN(created_at) FROM compute_tasks WHERE status='queued'").fetchone()[0]
        return {"states": {row["status"]: row["amount"] for row in rows}, "oldest_queued_at": oldest, "templates": len(self.repository.active_templates())}

    def _intervene(self, task_id: int, actor: str, reason: str, action: str, batch_key: str, mutation: Callable[[sqlite3.Connection, sqlite3.Row, str], None]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            before = dict(task)
            mutation(connection, task, now)
            after = dict(repository.task_by_id(task_id))
            repository.add_intervention(task_id=task_id, actor=actor, action=action, reason=reason, before=before, after=after, batch_key=batch_key, now=now)
            return after

    @staticmethod
    def _cancel_mutation(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
        if task["status"] not in {"queued", "running"}:
            raise ConflictError("当前任务状态不允许取消")
        status = "cancel_requested" if task["status"] == "running" else "cancelled"
        connection.execute("UPDATE compute_tasks SET status=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?", (status, None if status == "cancel_requested" else now, now, task["id"]))

    def _check_quota(self, repository: ComputeRepository, requested_by: str, now: datetime) -> None:
        quota = repository.quota("user", requested_by)
        if quota is None:
            return
        states = repository.count_user_states(requested_by)
        if states.get("queued", 0) >= int(quota["max_queued"]):
            raise ConflictError("用户排队任务配额已用尽")
        if states.get("running", 0) >= int(quota["max_running"]):
            raise ConflictError("用户运行任务配额已用尽")
        day_start = to_storage(now.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0))
        if repository.count_user_submissions_since(requested_by, day_start) >= int(quota["daily_submissions"]):
            raise ConflictError("用户当日提交配额已用尽")

    @staticmethod
    def _validate_schema(schema: dict[str, dict[str, Any]], defaults: dict[str, Any]) -> None:
        if not schema:
            raise ValidationError("参数模板至少包含一个参数")
        allowed = {"integer", "number", "string", "boolean"}
        for name, rule in schema.items():
            if not name or not isinstance(rule, dict) or rule.get("type") not in allowed:
                raise ValidationError(f"参数 {name or '<empty>'} 的规则不合法")
        if set(defaults) - set(schema):
            raise ValidationError("默认值包含未声明参数")

    def _validate_parameters(self, template: sqlite3.Row, supplied: dict[str, Any]) -> dict[str, Any]:
        schema = json.loads(template["parameter_schema_json"])
        values = {**json.loads(template["default_parameters_json"]), **supplied}
        unknown = set(values) - set(schema)
        if unknown:
            raise ValidationError("包含模板未声明的参数", context={"parameters": sorted(unknown)})
        normalized: dict[str, Any] = {}
        for name, rule in schema.items():
            if name not in values:
                if rule.get("required"):
                    raise ValidationError(f"缺少必填参数：{name}")
                continue
            value = values[name]
            kind = rule["type"]
            valid = {"integer": isinstance(value, int) and not isinstance(value, bool), "number": isinstance(value, (int, float)) and not isinstance(value, bool), "string": isinstance(value, str), "boolean": isinstance(value, bool)}[kind]
            if not valid:
                raise ValidationError(f"参数 {name} 类型不正确")
            if rule.get("minimum") is not None and value < rule["minimum"]:
                raise ValidationError(f"参数 {name} 小于允许的最小值")
            if rule.get("maximum") is not None and value > rule["maximum"]:
                raise ValidationError(f"参数 {name} 大于允许的最大值")
            if rule.get("choices") and value not in rule["choices"]:
                raise ValidationError(f"参数 {name} 不在允许的选项中")
            normalized[name] = value
        return normalized
