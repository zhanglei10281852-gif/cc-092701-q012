from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, Iterable

from app.compute.repository import ComputeRepository
from app.compute.schemas import ArtifactManifestItem, ArtifactStaging
from app.compute.storage import ArtifactBlobStore
from app.core.clock import Clock, SystemClock, to_storage
from app.core.config import Settings
from app.core.errors import ConflictError, GoneError, NotFoundError, PermissionDeniedError, ValidationError
from app.database import get_connection, transaction


def digest(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


class ComputeOperationsService:
    """管理计算模板、配额、任务租约、结果版本、成果文件清单和人工干预。"""

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None, settings: Settings | None = None, blob_store: ArtifactBlobStore | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        self.settings = settings or Settings.load()
        self.blob_store = blob_store or ArtifactBlobStore(self.settings.artifact_store_path)
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
        result["artifacts"] = self.repository.artifacts_for_task(task_id)
        result["result_events"] = self.repository.result_events(task_id)
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

    def complete(self, task_id: int, worker_id: str, result: dict[str, Any], metrics: dict[str, Any], artifacts: Iterable[ArtifactManifestItem] = (), receipt_key: str | None = None) -> dict[str, Any]:
        artifact_items = list(artifacts)
        request_hash = digest({"result": result, "metrics": metrics, "manifest": [self._manifest_fingerprint(item) for item in artifact_items]})
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            # 回执幂等优先于租约校验：任务成功后租约已清空，重放旧回执仍应返回同一份清单，
            # 而不是报“未由当前工作者持有”。
            if receipt_key:
                existing_receipt = repository.receipt(task_id, receipt_key)
                if existing_receipt is not None:
                    if existing_receipt["created_by"] != worker_id:
                        raise ConflictError("回执键属于其他工作者，不能用于重复提交")
                    if existing_receipt["request_hash"] != request_hash:
                        raise ConflictError("同一回执键对应了不同的结果或成果清单")
                    response = dict(repository.task_by_id(task_id))
                    response["artifacts"] = repository.artifacts_for_version(task_id, int(existing_receipt["result_version"]))
                    response["result_version"] = int(existing_receipt["result_version"])
                    response["receipt_key"] = receipt_key
                    response["idempotent_replay"] = True
                    return response
            if task["status"] != "running" or task["lease_owner"] != worker_id:
                raise ConflictError("任务未由当前工作者持有")
            # 先核对清单与内容：任一摘要/大小不符都会抛出异常，事务回滚，任务不会转为成功，
            # 且在全部条目核对通过前不写任何物理文件。
            candidate_expires = to_storage(now_value + timedelta(days=self.settings.artifact_candidate_retention_days))
            verified_artifacts: list[dict[str, Any]] = []
            for item in artifact_items:
                verified_artifacts.append(self._verify_manifest_item(repository, item, worker_id, now, candidate_expires))
            version = int(connection.execute("SELECT COALESCE(MAX(version),0)+1 FROM compute_results WHERE task_id=?", (task_id,)).fetchone()[0])
            connection.execute(
                "INSERT INTO compute_results(task_id,version,result_json,metrics_json,result_digest,created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                (task_id, version, json.dumps(result, ensure_ascii=False, sort_keys=True), json.dumps(metrics, ensure_ascii=False, sort_keys=True), digest({"result": result, "metrics": metrics}), worker_id, now),
            )
            for prepared in verified_artifacts:
                item = prepared["item"]
                if prepared["data"] is not None:
                    blob_sha, size_bytes, relpath = self.blob_store.put_bytes(
                        prepared["data"], declared_sha256=item.content_sha256, declared_size=item.size_bytes
                    )
                    repository.ensure_blob(content_sha256=blob_sha, size_bytes=size_bytes, storage_relpath=relpath, now=now)
                else:
                    blob_sha, size_bytes = prepared["blob_sha"], prepared["size_bytes"]
                row = repository.create_artifact(
                    task_id=task_id, result_version=version, receipt_key=receipt_key or "",
                    worker_path=item.worker_path, filename=item.filename, size_bytes=size_bytes,
                    content_sha256=blob_sha, purpose=item.purpose, content_type=item.content_type,
                    lifecycle_state="candidate", blob_sha256=blob_sha,
                    retention_expires_at=prepared["retention_expires_at"], now=now,
                )
                if item.staging_key:
                    repository.mark_staging_consumed(item.staging_key, task_id, now)
                if row["content_sha256"] != blob_sha:  # 防御性校验：落库摘要必须一致
                    raise ConflictError("成果清单落库摘要不一致，任务不得转为成功")
            if receipt_key:
                repository.create_receipt(task_id=task_id, receipt_key=receipt_key, request_hash=request_hash, result_version=version, created_by=worker_id, now=now)
            connection.execute(
                "UPDATE compute_tasks SET status='succeeded',current_result_version=?,lease_owner='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (version, now, now, task_id),
            )
            response = dict(repository.task_by_id(task_id))
            response["artifacts"] = repository.artifacts_for_version(task_id, version)
            response["result_version"] = version
            response["receipt_key"] = receipt_key or ""
            return response

    def stage_artifact(self, payload: ArtifactStaging) -> dict[str, Any]:
        """工作者预先上传临时成果文件，返回临时键；供后续完成回执引用。"""
        now_value = self.clock.now()
        now = to_storage(now_value)
        data = payload.decode_content()
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            existing = repository.staging(payload.staging_key)
            if existing is not None:
                if existing["consumed_at"] is not None:
                    raise ConflictError("暂存键已被完成回执消费")
                actual_sha = hashlib.sha256(data).hexdigest()
                if existing["blob_sha256"] != actual_sha or existing["filename"] != payload.filename:
                    raise ConflictError("同一暂存键对应了不同的文件内容或文件名")
                return dict(existing)
            blob_sha, size_bytes, relpath = self.blob_store.put_bytes(
                data, declared_sha256=payload.content_sha256, declared_size=payload.size_bytes
            )
            repository.ensure_blob(content_sha256=blob_sha, size_bytes=size_bytes, storage_relpath=relpath, now=now)
            expires_at = to_storage(now_value + timedelta(seconds=payload.ttl_seconds))
            return repository.create_staging(
                staging_key=payload.staging_key, worker_id=payload.worker_id, filename=payload.filename,
                purpose=payload.purpose, size_bytes=size_bytes, content_sha256=blob_sha, blob_sha256=blob_sha,
                expires_at=expires_at, now=now,
            )

    # ---- 成绩版本发布 / 撤回 -------------------------------------------------

    def publish_result(self, task_id: int, version: int, actor: str, reason: str) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            self._require_result(repository, task_id, version)
            current = repository.result_version(task_id, version)
            if current["publish_state"] == "published":
                raise ConflictError("该成绩版本已经处于发布状态")
            repository.set_result_publish_state(task_id, version, "published", now, actor=actor, reason=reason)
            expires = to_storage(now_value + timedelta(days=self.settings.artifact_published_retention_days))
            for artifact in repository.artifacts_for_version(task_id, version):
                repository.update_artifact_lifecycle(artifact["id"], lifecycle_state="published", retention_expires_at=expires, now=now)
            repository.add_result_event(task_id=task_id, result_version=version, action="published", actor=actor, reason=reason, now=now)
            return self._result_view(repository, task_id, version)

    def withdraw_result(self, task_id: int, version: int, actor: str, reason: str) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            self._require_result(repository, task_id, version)
            current = repository.result_version(task_id, version)
            if current["publish_state"] != "published":
                raise ConflictError("只有已发布的成绩版本可以撤回")
            repository.set_result_publish_state(task_id, version, "withdrawn", now, actor=actor, reason=reason)
            expires = to_storage(now_value + timedelta(days=self.settings.artifact_withdrawn_retention_days))
            for artifact in repository.artifacts_for_version(task_id, version):
                repository.update_artifact_lifecycle(artifact["id"], lifecycle_state="withdrawn", retention_expires_at=expires, now=now)
            repository.add_result_event(task_id=task_id, result_version=version, action="withdrawn", actor=actor, reason=reason, now=now)
            return self._result_view(repository, task_id, version)

    def grant_project_member(self, payload: dict[str, Any]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            return ComputeRepository(connection).grant_project_member(
                project_code=payload["project_code"], member=payload["member"], member_role=payload["member_role"],
                granted_by=payload["actor"], now=now,
            )

    # ---- 下载访问判定 --------------------------------------------------------

    def explain_artifact_access(self, artifact_id: int, requester: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            decision = self._access_decision(repository, artifact_id, requester, now)
            if decision["decision_code"] == "not_found":
                raise NotFoundError("成果文件不存在")
            repository.record_download(
                artifact_id=artifact_id, requester=requester,
                outcome="allowed" if decision["allowed"] else "denied",
                decision_code=decision["decision_code"], detail=decision["reason"], now=now,
            )
            return decision

    def download_artifact(self, artifact_id: int, requester: str) -> tuple[bytes, dict[str, Any], dict[str, Any]]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            decision = self._access_decision(repository, artifact_id, requester, now)
            if decision["decision_code"] == "not_found":
                raise NotFoundError("成果文件不存在")
            repository.record_download(
                artifact_id=artifact_id, requester=requester,
                outcome="allowed" if decision["allowed"] else "denied",
                decision_code=decision["decision_code"], detail=decision["reason"], now=now,
            )
            if not decision["allowed"]:
                if decision["decision_code"] == "artifact_purged":
                    raise GoneError(decision["reason"], context={"artifact_id": artifact_id})
                raise PermissionDeniedError(decision["reason"], context={"artifact_id": artifact_id, "decision_code": decision["decision_code"]})
            artifact = repository.artifact_by_id(artifact_id)
            blob = repository.blob(artifact["blob_sha256"])
        data = self.blob_store.read(blob["storage_relpath"])
        if hashlib.sha256(data).hexdigest() != artifact["content_sha256"] or len(data) != artifact["size_bytes"]:
            raise ConflictError("成果文件物理内容与清单摘要不符，已拒绝下载", context={"artifact_id": artifact_id})
        return data, dict(artifact), decision

    # ---- 清理 ----------------------------------------------------------------

    def purge_expired_artifacts(self, actor: str = "retention-worker", *, dry_run: bool = False) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        purged: list[dict[str, Any]] = []
        staging_removed: list[str] = []
        physical_to_delete: list[tuple[str, str]] = []
        protected = 0
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            for staged in repository.expired_staging(now):
                staging_removed.append(staged["staging_key"])
                if not dry_run:
                    repository.delete_staging(staged["staging_key"])
            for row in repository.purgeable_artifacts(now):
                purged.append({"artifact_id": row["id"], "task_id": row["task_id"], "result_version": row["result_version"],
                               "filename": row["filename"], "lifecycle_state": row["lifecycle_state"],
                               "retention_expires_at": row["retention_expires_at"]})
                if not dry_run:
                    repository.purge_artifact(row["id"], "超过保留期且无成绩版本引用", now)
            protected = repository.protected_expired_artifacts(now)
            if not dry_run:
                for blob in repository.unreferenced_blobs(now):
                    physical_to_delete.append((blob["content_sha256"], blob["storage_relpath"]))
                    repository.delete_blob(blob["content_sha256"])
        deleted_blobs: list[str] = []
        for blob_sha, relpath in physical_to_delete:
            if self.blob_store.delete(relpath):
                deleted_blobs.append(blob_sha)
        return {
            "actor": actor,
            "dry_run": dry_run,
            "finished_at": now,
            "temporary_staging_removed": staging_removed,
            "artifacts_purged": purged,
            "referenced_artifacts_retained": protected,
            "blobs_deleted": sorted(deleted_blobs),
        }

    # ---- 内部辅助 ------------------------------------------------------------

    def _verify_manifest_item(self, repository: ComputeRepository, item: ArtifactManifestItem, worker_id: str, now: str, retention_expires_at: str) -> dict[str, Any]:
        """核对清单条目；内联内容在内存中重算摘要，通过后才允许写物理文件。

        返回 {"item", "data"（内联）或 "blob_sha"/"size_bytes"（暂存）, "retention_expires_at"}。
        """
        if item.staging_key is not None:
            staged = repository.staging(item.staging_key)
            if staged is None:
                raise ValidationError("清单引用的暂存文件不存在", context={"staging_key": item.staging_key})
            if staged["consumed_at"] is not None:
                raise ConflictError("清单引用的暂存文件已被其他回执消费", context={"staging_key": item.staging_key})
            if staged["expires_at"] < now:
                raise ConflictError("清单引用的暂存文件已过保留期", context={"staging_key": item.staging_key})
            if staged["worker_id"] != worker_id:
                raise PermissionDeniedError("工作者不能引用其他工作者暂存的文件", context={"staging_key": item.staging_key})
            if staged["content_sha256"] != item.content_sha256 or staged["size_bytes"] != item.size_bytes:
                raise ConflictError("清单摘要或大小与暂存文件不符，任务不得转为成功", context={"filename": item.filename})
            return {"item": item, "data": None, "blob_sha": staged["blob_sha256"], "size_bytes": int(staged["size_bytes"]), "retention_expires_at": retention_expires_at}
        data = item.decode_content()
        actual_sha = hashlib.sha256(data).hexdigest()
        if actual_sha != item.content_sha256:
            raise ConflictError("成果文件摘要与实际内容不符，任务不得转为成功", context={"filename": item.filename, "declared_sha256": item.content_sha256, "actual_sha256": actual_sha})
        if len(data) != item.size_bytes:
            raise ConflictError("成果文件大小与实际内容不符，任务不得转为成功", context={"filename": item.filename, "declared_size": item.size_bytes, "actual_size": len(data)})
        return {"item": item, "data": data, "blob_sha": actual_sha, "size_bytes": len(data), "retention_expires_at": retention_expires_at}

    @staticmethod
    def _manifest_fingerprint(item: ArtifactManifestItem) -> dict[str, Any]:
        return {
            "worker_path": item.worker_path,
            "filename": item.filename,
            "size_bytes": item.size_bytes,
            "content_sha256": item.content_sha256,
            "purpose": item.purpose,
            "content_type": item.content_type,
            "source": "staging" if item.staging_key else "inline",
            "staging_key": item.staging_key or "",
        }

    @staticmethod
    def _relpath_for_sha(content_sha256: str) -> str:
        return f"{content_sha256[:2]}/{content_sha256[2:]}"

    @staticmethod
    def _require_result(repository: ComputeRepository, task_id: int, version: int) -> None:
        if repository.task_by_id(task_id) is None:
            raise NotFoundError("计算任务不存在")
        if repository.result_version(task_id, version) is None:
            raise NotFoundError("成绩版本不存在")

    def _result_view(self, repository: ComputeRepository, task_id: int, version: int) -> dict[str, Any]:
        result = dict(repository.result_version(task_id, version))
        result["artifacts"] = repository.artifacts_for_version(task_id, version)
        result["events"] = repository.result_events(task_id)
        return result

    def _access_decision(self, repository: ComputeRepository, artifact_id: int, requester: str, now: str) -> dict[str, Any]:
        artifact = repository.artifact_by_id(artifact_id)
        base: dict[str, Any] = {
            "artifact_id": artifact_id,
            "requester": requester,
            "now": now,
            "allowed": False,
            "task_id": None,
            "result_version": None,
            "is_current_version": False,
            "filename": None,
            "lifecycle_state": None,
            "publish_state": None,
            "retention_expires_at": None,
            "decision_code": "",
            "reason": "",
        }
        if artifact is None:
            base["decision_code"] = "not_found"
            base["reason"] = "成果文件不存在"
            return base
        result = repository.result_version(artifact["task_id"], artifact["result_version"])
        base.update({
            "task_id": artifact["task_id"],
            "result_version": artifact["result_version"],
            "is_current_version": artifact["current_result_version"] == artifact["result_version"],
            "filename": artifact["filename"],
            "lifecycle_state": artifact["lifecycle_state"],
            "publish_state": result["publish_state"] if result else None,
            "retention_expires_at": artifact["retention_expires_at"],
            "worker_path": artifact["worker_path"],
            "purpose": artifact["purpose"],
        })
        if artifact["purged_at"] is not None:
            base["decision_code"] = "artifact_purged"
            base["reason"] = f"成果文件已于 {artifact['purged_at']} 按规则 {artifact['purge_reason']} 清理"
            return base
        # 已发布的公开成绩永久保留，下载不受保留期限制；其余状态必须仍在保留期内。
        if base["publish_state"] != "published" and artifact["retention_expires_at"] < now:
            base["decision_code"] = "retention_expired"
            base["reason"] = f"成果文件已过保留期（{artifact['retention_expires_at']}），且未被公开成绩引用"
            return base
        if requester == "administrator":
            base["allowed"] = True
            base["decision_code"] = "allowed"
            base["reason"] = "管理员拥有全部成果的访问权限"
            return base
        membership = repository.project_member(artifact["project_code"], requester)
        is_owner = artifact["requested_by"] == requester
        member_role = membership["member_role"] if membership is not None else ""
        if not is_owner and membership is None:
            base["decision_code"] = "permission_denied"
            base["reason"] = f"请求者既不是提交人，也不是项目 {artifact['project_code']} 的授权教师或观察员"
            return base
        if artifact["lifecycle_state"] == "candidate" and membership is not None and member_role == "observer":
            base["decision_code"] = "candidate_not_published"
            base["reason"] = "该文件属于候选成绩版本，尚未发布，观察员不可访问"
            return base
        if artifact["lifecycle_state"] == "withdrawn" and membership is not None and member_role == "observer":
            base["decision_code"] = "version_withdrawn_for_observer"
            base["reason"] = "该成绩版本已撤回，观察员不可访问"
            return base
        base["allowed"] = True
        base["decision_code"] = "allowed"
        if is_owner:
            base["reason"] = "提交人访问自己任务的成果文件"
        elif member_role == "teacher":
            base["reason"] = "项目授权教师可访问在保留期内的成果文件"
        else:
            base["reason"] = "项目观察员可访问已发布且在保留期内的成果文件"
        return base

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
            if task["status"] not in {"failed", "cancelled"}:
                raise ConflictError("只有失败或已取消任务可以人工重试")
            chosen = task["priority"] if priority is None else priority
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
