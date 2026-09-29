from __future__ import annotations

import json
import sqlite3
from typing import Any, Iterable


class ComputeRepository:
    """封装计算任务运营领域的 SQLite 读写。"""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    def template_by_code(self, code: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_templates WHERE code=?", (code,)).fetchone()

    def template_by_id(self, template_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_templates WHERE id=?", (template_id,)).fetchone()

    def active_templates(self) -> list[dict[str, Any]]:
        rows = self.connection.execute("SELECT * FROM compute_templates WHERE active=1 ORDER BY code,version").fetchall()
        return [dict(row) for row in rows]

    def create_template(self, *, code: str, name: str, algorithm: str, parameter_schema: dict[str, Any], defaults: dict[str, Any], max_runtime_seconds: int, max_attempts: int, created_by: str, now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO compute_templates(code,name,algorithm,version,parameter_schema_json,default_parameters_json,max_runtime_seconds,max_attempts,active,created_by,created_at,updated_at) VALUES(?,?,?,1,?,?,?,?,1,?,?,?)",
            (code, name, algorithm, json.dumps(parameter_schema, ensure_ascii=False, sort_keys=True), json.dumps(defaults, ensure_ascii=False, sort_keys=True), max_runtime_seconds, max_attempts, created_by, now, now),
        )
        return dict(self.template_by_id(cursor.lastrowid))

    def quota(self, subject_type: str, subject_key: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_quotas WHERE subject_type=? AND subject_key=?", (subject_type, subject_key)).fetchone()

    def upsert_quota(self, *, subject_type: str, subject_key: str, max_queued: int, max_running: int, daily_submissions: int, actor: str, now: str) -> dict[str, Any]:
        self.connection.execute(
            "INSERT INTO compute_quotas(subject_type,subject_key,max_queued,max_running,daily_submissions,updated_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(subject_type,subject_key) DO UPDATE SET max_queued=excluded.max_queued,max_running=excluded.max_running,daily_submissions=excluded.daily_submissions,updated_by=excluded.updated_by,updated_at=excluded.updated_at",
            (subject_type, subject_key, max_queued, max_running, daily_submissions, actor, now, now),
        )
        return dict(self.quota(subject_type, subject_key))

    def count_user_states(self, requested_by: str) -> dict[str, int]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM compute_tasks WHERE requested_by=? GROUP BY status", (requested_by,)).fetchall()
        return {str(row["status"]): int(row["amount"]) for row in rows}

    def count_user_submissions_since(self, requested_by: str, since: str) -> int:
        return int(self.connection.execute("SELECT COUNT(*) FROM compute_tasks WHERE requested_by=? AND created_at>=?", (requested_by, since)).fetchone()[0])

    def task_by_id(self, task_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT t.*,tpl.code AS template_code,tpl.algorithm AS template_algorithm FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id WHERE t.id=?", (task_id,)).fetchone()

    def task_by_idempotency(self, requested_by: str, key: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_tasks WHERE requested_by=? AND idempotency_key=?", (requested_by, key)).fetchone()

    def create_task(self, *, template_id: int, project_code: str, requested_by: str, parameters: dict[str, Any], parameter_digest: str, priority: int, idempotency_key: str, max_attempts: int, now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO compute_tasks(template_id,project_code,requested_by,parameters_json,parameter_digest,priority,idempotency_key,status,attempt_count,max_attempts,available_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,'queued',0,?,?,?,?)",
            (template_id, project_code, requested_by, json.dumps(parameters, ensure_ascii=False, sort_keys=True), parameter_digest, priority, idempotency_key, max_attempts, now, now, now),
        )
        return dict(self.task_by_id(cursor.lastrowid))

    def queued_candidate(self, capabilities: Iterable[str], now: str) -> sqlite3.Row | None:
        capability_list = sorted(set(capabilities))
        params: list[Any] = [now]
        condition = ""
        if capability_list:
            placeholders = ",".join("?" for _ in capability_list)
            condition = f" AND tpl.algorithm IN ({placeholders})"
            params.extend(capability_list)
        return self.connection.execute(
            "SELECT t.*,tpl.algorithm AS template_algorithm FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id WHERE t.status='queued' AND t.available_at<=?" + condition + " ORDER BY t.priority DESC,t.created_at ASC,t.id ASC LIMIT 1",
            params,
        ).fetchone()

    def result_version(self, task_id: int, version: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_results WHERE task_id=? AND version=?", (task_id, version)).fetchone()

    def result_versions(self, task_id: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM compute_results WHERE task_id=? ORDER BY version", (task_id,)).fetchall()]

    def receipt(self, task_id: int, receipt_key: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_result_receipts WHERE task_id=? AND receipt_key=?", (task_id, receipt_key)).fetchone()

    def add_receipt(self, *, task_id: int, receipt_key: str, result_version: int, created_by: str, now: str) -> None:
        self.connection.execute(
            "INSERT INTO compute_result_receipts(task_id,receipt_key,result_version,created_by,created_at) VALUES(?,?,?,?,?)",
            (task_id, receipt_key, result_version, created_by, now),
        )

    def insert_result(self, *, task_id: int, version: int, result: dict[str, Any], metrics: dict[str, Any], result_digest: str, created_by: str, now: str, lifecycle_state: str, retention_until: str) -> None:
        self.connection.execute(
            "INSERT INTO compute_results(task_id,version,result_json,metrics_json,result_digest,lifecycle_state,retention_until,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (task_id, version, json.dumps(result, ensure_ascii=False, sort_keys=True), json.dumps(metrics, ensure_ascii=False, sort_keys=True), result_digest, lifecycle_state, retention_until, created_by, now),
        )

    def set_result_lifecycle(self, *, task_id: int, version: int, state: str, retention_until: str, published_at: str | None, withdrawn_at: str | None, withdraw_reason: str) -> None:
        self.connection.execute(
            "UPDATE compute_results SET lifecycle_state=?,retention_until=?,published_at=COALESCE(?,published_at),withdrawn_at=?,withdraw_reason=? WHERE task_id=? AND version=?",
            (state, retention_until, published_at, withdrawn_at, withdraw_reason, task_id, version),
        )

    # ----- 成果文件：blob / 暂存 / 清单 -----

    def blob_by_digest(self, sha256: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_artifact_blobs WHERE sha256=?", (sha256,)).fetchone()

    def blob_by_id(self, blob_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_artifact_blobs WHERE id=?", (blob_id,)).fetchone()

    def insert_blob(self, *, sha256: str, size_bytes: int, storage_relpath: str, now: str) -> int:
        cursor = self.connection.execute(
            "INSERT INTO compute_artifact_blobs(sha256,size_bytes,storage_relpath,created_at) VALUES(?,?,?,?)",
            (sha256, size_bytes, storage_relpath, now),
        )
        return int(cursor.lastrowid)

    def create_staging(self, *, upload_id: str, blob_id: int | None, storage_relpath: str, filename: str, purpose: str, size_bytes: int, sha256: str, uploaded_by: str, now: str, expires_at: str) -> None:
        self.connection.execute(
            "INSERT INTO compute_artifact_staging(upload_id,blob_id,storage_relpath,filename,purpose,size_bytes,sha256,uploaded_by,created_at,expires_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (upload_id, blob_id, storage_relpath, filename, purpose, size_bytes, sha256, uploaded_by, now, expires_at),
        )

    def staging_by_upload(self, upload_id: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_artifact_staging WHERE upload_id=?", (upload_id,)).fetchone()

    def delete_staging(self, upload_id: str) -> None:
        self.connection.execute("DELETE FROM compute_artifact_staging WHERE upload_id=?", (upload_id,))

    def expired_staging(self, now: str) -> list[sqlite3.Row]:
        return self.connection.execute("SELECT * FROM compute_artifact_staging WHERE expires_at<=? ORDER BY id", (now,)).fetchall()

    def insert_result_artifact(self, *, task_id: int, result_version: int, blob_id: int, declared_path: str, filename: str, purpose: str, declared_size: int, size_bytes: int, sha256: str, state: str, retention_until: str, created_by: str, now: str) -> None:
        self.connection.execute(
            "INSERT INTO compute_result_artifacts(task_id,result_version,blob_id,declared_path,filename,purpose,declared_size,size_bytes,sha256,state,retention_until,created_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (task_id, result_version, blob_id, declared_path, filename, purpose, declared_size, size_bytes, sha256, state, retention_until, created_by, now, now),
        )

    def result_artifacts(self, task_id: int, version: int | None = None) -> list[dict[str, Any]]:
        if version is None:
            rows = self.connection.execute("SELECT * FROM compute_result_artifacts WHERE task_id=? ORDER BY result_version,id", (task_id,)).fetchall()
        else:
            rows = self.connection.execute("SELECT * FROM compute_result_artifacts WHERE task_id=? AND result_version=? ORDER BY id", (task_id, version)).fetchall()
        return [dict(row) for row in rows]

    def artifact_by_filename(self, task_id: int, version: int, filename: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_result_artifacts WHERE task_id=? AND result_version=? AND filename=?", (task_id, version, filename)).fetchone()

    def artifact_by_id(self, artifact_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_result_artifacts WHERE id=?", (artifact_id,)).fetchone()

    def set_artifacts_lifecycle(self, *, task_id: int, version: int, state: str, retention_until: str, published_at: str | None, withdrawn_at: str | None, now: str) -> None:
        self.connection.execute(
            "UPDATE compute_result_artifacts SET state=?,retention_until=?,published_at=COALESCE(?,published_at),withdrawn_at=?,updated_at=? WHERE task_id=? AND result_version=?",
            (state, retention_until, published_at, withdrawn_at, now, task_id, version),
        )

    def unreferenced_blobs(self) -> list[sqlite3.Row]:
        return self.connection.execute(
            "SELECT b.* FROM compute_artifact_blobs b WHERE NOT EXISTS (SELECT 1 FROM compute_result_artifacts a WHERE a.blob_id=b.id) ORDER BY b.id"
        ).fetchall()

    def expired_result_artifacts(self, state: str, now: str) -> list[sqlite3.Row]:
        return self.connection.execute(
            "SELECT * FROM compute_result_artifacts WHERE state=? AND retention_until<>'' AND retention_until<=? ORDER BY id",
            (state, now),
        ).fetchall()

    def delete_result_artifact(self, artifact_id: int) -> None:
        self.connection.execute("DELETE FROM compute_result_artifacts WHERE id=?", (artifact_id,))

    def blob_is_referenced(self, blob_id: int) -> bool:
        return self.connection.execute("SELECT 1 FROM compute_result_artifacts WHERE blob_id=? LIMIT 1", (blob_id,)).fetchone() is not None

    # ----- 项目成员授权 -----

    def grant_project_member(self, *, project_code: str, member: str, role: str, granted_by: str, now: str) -> dict[str, Any]:
        self.connection.execute(
            "INSERT INTO compute_project_members(project_code,member,role,granted_by,created_at) VALUES(?,?,?,?,?) ON CONFLICT(project_code,member) DO UPDATE SET role=excluded.role,granted_by=excluded.granted_by",
            (project_code, member, role, granted_by, now),
        )
        row = self.connection.execute("SELECT * FROM compute_project_members WHERE project_code=? AND member=?", (project_code, member)).fetchone()
        return dict(row)

    def project_member(self, project_code: str, member: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_project_members WHERE project_code=? AND member=?", (project_code, member)).fetchone()

    def interventions(self, task_id: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM compute_interventions WHERE task_id=? ORDER BY id", (task_id,)).fetchall()]

    def add_intervention(self, *, task_id: int, actor: str, action: str, reason: str, before: dict[str, Any], after: dict[str, Any], batch_key: str, now: str) -> None:
        self.connection.execute(
            "INSERT INTO compute_interventions(task_id,actor,action,reason,before_json,after_json,batch_key,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (task_id, actor, action, reason, json.dumps(before, ensure_ascii=False, sort_keys=True), json.dumps(after, ensure_ascii=False, sort_keys=True), batch_key, now),
        )

    def list_tasks(self, *, status: str | None, project_code: str | None, requested_by: str | None, limit: int) -> list[dict[str, Any]]:
        clauses: list[str] = []
        values: list[Any] = []
        if status:
            clauses.append("t.status=?")
            values.append(status)
        if project_code:
            clauses.append("t.project_code=?")
            values.append(project_code)
        if requested_by:
            clauses.append("t.requested_by=?")
            values.append(requested_by)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        values.append(limit)
        rows = self.connection.execute(
            "SELECT t.*,tpl.code AS template_code,tpl.algorithm AS template_algorithm FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id" + where + " ORDER BY t.priority DESC,t.created_at DESC,t.id DESC LIMIT ?",
            values,
        ).fetchall()
        return [dict(row) for row in rows]
