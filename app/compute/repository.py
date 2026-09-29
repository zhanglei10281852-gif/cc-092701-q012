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

    def result_versions(self, task_id: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM compute_results WHERE task_id=? ORDER BY version", (task_id,)).fetchall()]

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

    # ---- 成果清单 / 生命周期 -------------------------------------------------

    def ensure_blob(self, *, content_sha256: str, size_bytes: int, storage_relpath: str, now: str) -> None:
        self.connection.execute(
            "INSERT OR IGNORE INTO compute_artifact_blobs(content_sha256,size_bytes,storage_relpath,created_at) VALUES(?,?,?,?)",
            (content_sha256, size_bytes, storage_relpath, now),
        )

    def blob(self, content_sha256: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_artifact_blobs WHERE content_sha256=?", (content_sha256,)).fetchone()

    def unreferenced_blobs(self, now: str) -> list[sqlite3.Row]:
        return self.connection.execute(
            """
            SELECT b.* FROM compute_artifact_blobs b
            LEFT JOIN compute_artifacts a
              ON a.blob_sha256=b.content_sha256 AND a.purged_at IS NULL
            LEFT JOIN compute_artifact_staging s
              ON s.blob_sha256=b.content_sha256 AND s.consumed_at IS NULL AND s.expires_at>=?
            WHERE a.id IS NULL AND s.staging_key IS NULL
            """,
            (now,),
        ).fetchall()

    def delete_blob(self, content_sha256: str) -> None:
        self.connection.execute("DELETE FROM compute_artifact_blobs WHERE content_sha256=?", (content_sha256,))

    def create_staging(self, *, staging_key: str, worker_id: str, filename: str, purpose: str, size_bytes: int, content_sha256: str, blob_sha256: str, expires_at: str, now: str) -> dict[str, Any]:
        self.connection.execute(
            "INSERT INTO compute_artifact_staging(staging_key,worker_id,filename,purpose,size_bytes,content_sha256,blob_sha256,expires_at,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (staging_key, worker_id, filename, purpose, size_bytes, content_sha256, blob_sha256, expires_at, now),
        )
        return dict(self.staging(staging_key))

    def staging(self, staging_key: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_artifact_staging WHERE staging_key=?", (staging_key,)).fetchone()

    def mark_staging_consumed(self, staging_key: str, task_id: int, now: str) -> None:
        self.connection.execute(
            "UPDATE compute_artifact_staging SET consumed_at=?,consumed_by_task=? WHERE staging_key=?",
            (now, task_id, staging_key),
        )

    def expired_staging(self, now: str) -> list[sqlite3.Row]:
        return self.connection.execute(
            "SELECT * FROM compute_artifact_staging WHERE consumed_at IS NULL AND expires_at<? ORDER BY expires_at,staging_key",
            (now,),
        ).fetchall()

    def delete_staging(self, staging_key: str) -> None:
        self.connection.execute("DELETE FROM compute_artifact_staging WHERE staging_key=?", (staging_key,))

    def receipt(self, task_id: int, receipt_key: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_task_receipts WHERE task_id=? AND receipt_key=?", (task_id, receipt_key)).fetchone()

    def create_receipt(self, *, task_id: int, receipt_key: str, request_hash: str, result_version: int, created_by: str, now: str) -> None:
        self.connection.execute(
            "INSERT INTO compute_task_receipts(task_id,receipt_key,request_hash,result_version,created_by,created_at) VALUES(?,?,?,?,?,?)",
            (task_id, receipt_key, request_hash, result_version, created_by, now),
        )

    def create_artifact(self, *, task_id: int, result_version: int, receipt_key: str, worker_path: str, filename: str, size_bytes: int, content_sha256: str, purpose: str, content_type: str, lifecycle_state: str, blob_sha256: str, retention_expires_at: str, now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO compute_artifacts(task_id,result_version,receipt_key,worker_path,filename,size_bytes,content_sha256,purpose,content_type,lifecycle_state,blob_sha256,retention_expires_at,last_verified_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (task_id, result_version, receipt_key, worker_path, filename, size_bytes, content_sha256, purpose, content_type, lifecycle_state, blob_sha256, retention_expires_at, now, now, now),
        )
        return dict(self.artifact_by_id(cursor.lastrowid))

    def artifact_by_id(self, artifact_id: int) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT a.*,t.project_code AS project_code,t.requested_by AS requested_by,t.current_result_version AS current_result_version FROM compute_artifacts a JOIN compute_tasks t ON t.id=a.task_id WHERE a.id=?",
            (artifact_id,),
        ).fetchone()

    def artifacts_for_task(self, task_id: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM compute_artifacts WHERE task_id=? ORDER BY result_version,id", (task_id,)).fetchall()]

    def artifacts_for_version(self, task_id: int, result_version: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM compute_artifacts WHERE task_id=? AND result_version=? ORDER BY id", (task_id, result_version)).fetchall()]

    def update_artifact_lifecycle(self, artifact_id: int, *, lifecycle_state: str, retention_expires_at: str, now: str) -> None:
        self.connection.execute(
            "UPDATE compute_artifacts SET lifecycle_state=?,retention_expires_at=?,updated_at=? WHERE id=?",
            (lifecycle_state, retention_expires_at, now, artifact_id),
        )

    def touch_artifact_verified(self, artifact_id: int, now: str) -> None:
        self.connection.execute("UPDATE compute_artifacts SET last_verified_at=? WHERE id=?", (now, artifact_id))

    def purge_artifact(self, artifact_id: int, reason: str, now: str) -> None:
        # 保留成果行作为墓碑（用于 410 与审计），仅解除对物理 blob 的引用；
        # 清单声明的 content_sha256/size_bytes 仍保留。
        self.connection.execute(
            "UPDATE compute_artifacts SET purged_at=?,purge_reason=?,blob_sha256=NULL,updated_at=? WHERE id=? AND purged_at IS NULL",
            (now, reason, now, artifact_id),
        )

    def purgeable_artifacts(self, now: str) -> list[sqlite3.Row]:
        """已过保留期、且未被公开成绩或当前候选成绩版本引用的成果行。

        - published 永远不可清理（公开成绩引用必须保留）；
        - withdrawn 已解除公开引用，即使仍是 current_result_version，过撤回期也可清理；
        - candidate 若仍是任务当前成绩版本则必须保留。
        """
        return self.connection.execute(
            """
            SELECT a.* FROM compute_artifacts a
            JOIN compute_tasks t ON t.id=a.task_id
            LEFT JOIN compute_results r ON r.task_id=a.task_id AND r.version=a.result_version
            WHERE a.purged_at IS NULL
              AND a.retention_expires_at<?
              AND COALESCE(r.publish_state,'candidate')<>'published'
              AND (COALESCE(r.publish_state,'candidate')='withdrawn'
                   OR t.current_result_version IS NULL
                   OR t.current_result_version<>a.result_version)
            ORDER BY a.retention_expires_at,a.id
            """,
            (now,),
        ).fetchall()

    def result_version(self, task_id: int, version: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_results WHERE task_id=? AND version=?", (task_id, version)).fetchone()

    def result_events(self, task_id: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM compute_result_events WHERE task_id=? ORDER BY id", (task_id,)).fetchall()]

    def protected_expired_artifacts(self, now: str) -> int:
        """已过保留期但必须保留的成果数量：公开成绩（永久）或仍为当前版本的候选成绩。"""
        row = self.connection.execute(
            """
            SELECT COUNT(*) AS amount FROM compute_artifacts a
            JOIN compute_tasks t ON t.id=a.task_id
            LEFT JOIN compute_results r ON r.task_id=a.task_id AND r.version=a.result_version
            WHERE a.purged_at IS NULL AND a.retention_expires_at<?
              AND (COALESCE(r.publish_state,'candidate')='published'
                   OR (COALESCE(r.publish_state,'candidate')='candidate'
                       AND t.current_result_version=a.result_version))
            """,
            (now,),
        ).fetchone()
        return int(row["amount"])

    def set_result_publish_state(self, task_id: int, version: int, state: str, now: str, *, actor: str = "", reason: str = "") -> None:
        if state == "published":
            self.connection.execute(
                "UPDATE compute_results SET publish_state='published',published_at=?,published_by=?,withdrawn_at='',withdrawn_by='',withdraw_reason='' WHERE task_id=? AND version=?",
                (now, actor, task_id, version),
            )
        elif state == "withdrawn":
            self.connection.execute(
                "UPDATE compute_results SET publish_state='withdrawn',withdrawn_at=?,withdrawn_by=?,withdraw_reason=? WHERE task_id=? AND version=?",
                (now, actor, reason, task_id, version),
            )
        else:
            self.connection.execute(
                "UPDATE compute_results SET publish_state='candidate',published_at='',published_by='',withdrawn_at='',withdrawn_by='',withdraw_reason='' WHERE task_id=? AND version=?",
                (task_id, version),
            )

    def add_result_event(self, *, task_id: int, result_version: int, action: str, actor: str, reason: str, now: str) -> None:
        self.connection.execute(
            "INSERT INTO compute_result_events(task_id,result_version,action,actor,reason,created_at) VALUES(?,?,?,?,?,?)",
            (task_id, result_version, action, actor, reason, now),
        )

    def grant_project_member(self, *, project_code: str, member: str, member_role: str, granted_by: str, now: str) -> dict[str, Any]:
        self.connection.execute(
            "INSERT INTO compute_project_members(project_code,member,member_role,granted_by,created_at,updated_at) VALUES(?,?,?,?,?,?) ON CONFLICT(project_code,member) DO UPDATE SET member_role=excluded.member_role,granted_by=excluded.granted_by,updated_at=excluded.updated_at",
            (project_code, member, member_role, granted_by, now, now),
        )
        return dict(self.connection.execute("SELECT * FROM compute_project_members WHERE project_code=? AND member=?", (project_code, member)).fetchone())

    def project_member(self, project_code: str, member: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_project_members WHERE project_code=? AND member=?", (project_code, member)).fetchone()

    def record_download(self, *, artifact_id: int, requester: str, outcome: str, decision_code: str, detail: str, now: str) -> None:
        self.connection.execute(
            "INSERT INTO compute_artifact_downloads(artifact_id,requester,outcome,decision_code,detail,created_at) VALUES(?,?,?,?,?,?)",
            (artifact_id, requester, outcome, decision_code, detail, now),
        )
