from __future__ import annotations

import base64
import hashlib
from dataclasses import replace
from datetime import UTC, datetime, timedelta

from app.compute.service import ComputeOperationsService
from app.core.clock import FrozenClock
from app.core.config import Settings
from app.database import get_connection

from tests.test_compute_operations import TEMPLATE, create_template, submit_payload


def b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


LOG = b"2026-09-29 solver started\nconverged at step 42\n"
SIM = b"\x00\x01\x02simulation-binary\x03"


def artifact_item(*, filename: str, data: bytes, purpose: str, worker_path: str, declared_sha: str | None = None, declared_size: int | None = None) -> dict:
    return {
        "worker_path": worker_path,
        "filename": filename,
        "size_bytes": declared_size if declared_size is not None else len(data),
        "content_sha256": declared_sha if declared_sha is not None else sha(data),
        "purpose": purpose,
        "content_type": "text/plain",
        "content_base64": b64(data),
    }


def complete_with_artifacts(client, task_id: int, *, receipt_key: str = "receipt-000001", artifacts=None, result=None, worker: str = "w1"):
    return client.post(
        f"/api/compute/tasks/{task_id}/complete",
        json={
            "worker_id": worker,
            "result": result if result is not None else {"score": 90},
            "metrics": {"seconds": 3},
            "receipt_key": receipt_key,
            "artifacts": artifacts if artifacts is not None else [
                artifact_item(filename="run.log", data=LOG, purpose="运行日志", worker_path="/work/out/run.log"),
                artifact_item(filename="sim.bin", data=SIM, purpose="仿真数据", worker_path="/work/out/sim.bin"),
            ],
        },
    )


def claim_one(client, task_id: int | None = None, worker: str = "w1"):
    claimed = client.post("/api/compute/tasks/claim", json={"worker_id": worker, "capabilities": ["solver-a"], "lease_seconds": 300})
    assert claimed.status_code == 200 and claimed.json()["task"] is not None
    return claimed.json()["task"]


def prepare_task(client) -> dict:
    create_template(client)
    created = client.post("/api/compute/tasks", json=submit_payload("artifact-req-0001"))
    assert created.status_code == 202
    return claim_one(client, created.json()["id"])


# ---- 清单落库 ---------------------------------------------------------------

def test_manifest_verified_and_persisted_with_result_version(client):
    task = prepare_task(client)
    completed = complete_with_artifacts(client, task["id"])
    assert completed.status_code == 200, completed.text
    body = completed.json()
    assert body["status"] == "succeeded"
    assert body["result_version"] == 1
    details = client.get(f"/api/compute/task-details/{task['id']}").json()
    assert details["current_result_version"] == 1
    artifacts = details["artifacts"]
    assert [item["filename"] for item in artifacts] == ["run.log", "sim.bin"]
    log = next(item for item in artifacts if item["filename"] == "run.log")
    assert log["lifecycle_state"] == "candidate"
    assert log["worker_path"] == "/work/out/run.log"
    assert log["purpose"] == "运行日志"
    assert log["size_bytes"] == len(LOG)
    assert log["content_sha256"] == sha(LOG)
    assert log["receipt_key"] == "receipt-000001"


def test_digest_mismatch_blocks_success_and_leaves_no_blob(client):
    task = prepare_task(client)
    bad = [artifact_item(filename="run.log", data=LOG, purpose="运行日志", worker_path="/work/run.log", declared_sha="a" * 64)]
    response = complete_with_artifacts(client, task["id"], artifacts=bad)
    assert response.status_code == 409
    assert response.json()["error"]["context"]["actual_sha256"] == sha(LOG)
    details = client.get(f"/api/compute/task-details/{task['id']}").json()
    assert details["status"] == "running"
    assert details["artifacts"] == []
    assert details["results"] == []
    store = client  # 物理存储不应留下任何文件
    from app.core.config import Settings
    root = Settings.load().artifact_store_path
    assert not root.exists() or not any(root.rglob("*"))


def test_size_mismatch_blocks_success(client):
    task = prepare_task(client)
    bad = [artifact_item(filename="run.log", data=LOG, purpose="运行日志", worker_path="/work/run.log", declared_size=999)]
    response = complete_with_artifacts(client, task["id"], artifacts=bad)
    assert response.status_code == 409
    details = client.get(f"/api/compute/task-details/{task['id']}").json()
    assert details["status"] == "running"


# ---- 回执幂等 ---------------------------------------------------------------

def test_duplicate_receipt_yields_single_manifest(client):
    task = prepare_task(client)
    first = complete_with_artifacts(client, task["id"])
    second = complete_with_artifacts(client, task["id"])
    assert first.status_code == second.status_code == 200
    assert second.json()["result_version"] == first.json()["result_version"]
    assert second.json()["idempotent_replay"] is True
    details = client.get(f"/api/compute/task-details/{task['id']}").json()
    assert len(details["artifacts"]) == 2  # 同一清单只落一次（两个条目）
    assert len(details["results"]) == 1


def test_same_receipt_key_with_different_payload_rejected(client):
    task = prepare_task(client)
    assert complete_with_artifacts(client, task["id"]).status_code == 200
    conflict = complete_with_artifacts(client, task["id"], result={"score": 1})
    assert conflict.status_code == 409


def test_artifacts_require_receipt_key(client):
    task = prepare_task(client)
    response = client.post(
        f"/api/compute/tasks/{task['id']}/complete",
        json={"worker_id": "w1", "result": {"score": 1}, "artifacts": [
            artifact_item(filename="run.log", data=LOG, purpose="日志", worker_path="/work/run.log"),
        ]},
    )
    assert response.status_code == 422


# ---- 暂存区 -----------------------------------------------------------------

def test_staging_flow_and_consumption(client):
    create_template(client)
    staged = client.post("/api/compute/artifacts/staging", json={
        "staging_key": "stage-0001", "worker_id": "w1", "filename": "run.log", "purpose": "运行日志",
        "size_bytes": len(LOG), "content_sha256": sha(LOG), "content_base64": b64(LOG), "ttl_seconds": 600,
    })
    assert staged.status_code == 201, staged.text
    created = client.post("/api/compute/tasks", json=submit_payload("artifact-req-stage"))
    task = claim_one(client, created.json()["id"])
    completed = client.post(f"/api/compute/tasks/{task['id']}/complete", json={
        "worker_id": "w1", "result": {"score": 80}, "receipt_key": "receipt-stage-1",
        "artifacts": [{
            "worker_path": "/work/run.log", "filename": "run.log", "size_bytes": len(LOG),
            "content_sha256": sha(LOG), "purpose": "运行日志", "staging_key": "stage-0001",
        }],
    })
    assert completed.status_code == 200, completed.text
    # 暂存键不能重复消费
    again = client.post("/api/compute/artifacts/staging", json={
        "staging_key": "stage-0001", "worker_id": "w1", "filename": "run.log", "purpose": "运行日志",
        "size_bytes": len(LOG), "content_sha256": sha(LOG), "content_base64": b64(LOG),
    })
    assert again.status_code == 409


def test_staging_other_worker_rejected(client):
    prepare_task(client)
    client.post("/api/compute/artifacts/staging", json={
        "staging_key": "stage-owner", "worker_id": "w9", "filename": "run.log", "purpose": "日志",
        "size_bytes": len(LOG), "content_sha256": sha(LOG), "content_base64": b64(LOG),
    })
    created = client.post("/api/compute/tasks", json=submit_payload("artifact-req-owner"))
    task = claim_one(client, created.json()["id"], worker="w1")
    response = client.post(f"/api/compute/tasks/{task['id']}/complete", json={
        "worker_id": "w1", "result": {}, "receipt_key": "receipt-owner-1",
        "artifacts": [{
            "worker_path": "/work/run.log", "filename": "run.log", "size_bytes": len(LOG),
            "content_sha256": sha(LOG), "purpose": "日志", "staging_key": "stage-owner",
        }],
    })
    assert response.status_code == 403


# ---- 权限与访问说明 ----------------------------------------------------------

def _setup_published(client) -> tuple[int, int]:
    task = prepare_task(client)
    assert complete_with_artifacts(client, task["id"]).status_code == 200
    return task["id"], 1


def test_download_permissions_and_explanation(client):
    task_id, version = _setup_published(client)
    artifact_id = client.get(f"/api/compute/task-details/{task_id}").json()["artifacts"][0]["id"]

    # 提交人可以下载候选版本
    owner = client.get(f"/api/compute/artifacts/{artifact_id}/download", params={"requester": "researcher-1"})
    assert owner.status_code == 200
    assert owner.content == LOG

    # 陌生人无权限：说明接口返回拒绝原因（不强制 403，下载接口才强制）
    stranger = client.get(f"/api/compute/artifacts/{artifact_id}/access", params={"requester": "nobody"})
    assert stranger.status_code == 200
    assert stranger.json()["allowed"] is False
    assert stranger.json()["decision_code"] == "permission_denied"
    stranger_download = client.get(f"/api/compute/artifacts/{artifact_id}/download", params={"requester": "nobody"})
    assert stranger_download.status_code == 403

    # 观察员在候选阶段不可见
    client.put("/api/compute/project-members", json={"project_code": "project-a", "member": "obs-1", "member_role": "observer", "actor": "administrator"})
    observer_candidate = client.get(f"/api/compute/artifacts/{artifact_id}/access", params={"requester": "obs-1"})
    assert observer_candidate.status_code == 200
    assert observer_candidate.json()["allowed"] is False
    assert observer_candidate.json()["decision_code"] == "candidate_not_published"

    # 教师在候选阶段可下载
    client.put("/api/compute/project-members", json={"project_code": "project-a", "member": "teacher-1", "member_role": "teacher", "actor": "administrator"})
    teacher = client.get(f"/api/compute/artifacts/{artifact_id}/download", params={"requester": "teacher-1"})
    assert teacher.status_code == 200 and teacher.content == LOG

    # 发布后观察员可见，说明接口标注成绩版本与允许原因
    published = client.post(f"/api/compute/tasks/{task_id}/results/{version}/publish", json={"actor": "administrator", "reason": "期末成绩公布"})
    assert published.status_code == 200
    explanation = client.get(f"/api/compute/artifacts/{artifact_id}/access", params={"requester": "obs-1"})
    assert explanation.status_code == 200
    body = explanation.json()
    assert body["allowed"] is True
    assert body["task_id"] == task_id
    assert body["result_version"] == version
    assert body["publish_state"] == "published"
    assert body["lifecycle_state"] == "published"
    assert body["is_current_version"] is True

    # 撤回后观察员再次被拒，教师仍可在撤回保留期内访问
    withdrawn = client.post(f"/api/compute/tasks/{task_id}/results/{version}/withdraw", json={"actor": "administrator", "reason": "成绩复核暂停"})
    assert withdrawn.status_code == 200
    obs_after = client.get(f"/api/compute/artifacts/{artifact_id}/access", params={"requester": "obs-1"})
    assert obs_after.status_code == 200
    assert obs_after.json()["allowed"] is False
    assert obs_after.json()["decision_code"] == "version_withdrawn_for_observer"
    obs_download = client.get(f"/api/compute/artifacts/{artifact_id}/download", params={"requester": "obs-1"})
    assert obs_download.status_code == 403
    teacher_after = client.get(f"/api/compute/artifacts/{artifact_id}/download", params={"requester": "teacher-1"})
    assert teacher_after.status_code == 200


# ---- 保留期与清理（直接使用冻结时钟） ----------------------------------------

def _service_with_clock(clock: FrozenClock, *, tmp_store, candidate_days=30, withdrawn_days=30, published_days=3650, temporary_hours=24):
    settings = replace(
        Settings.load(),
        artifact_store_path=tmp_store,
        artifact_candidate_retention_days=candidate_days,
        artifact_withdrawn_retention_days=withdrawn_days,
        artifact_published_retention_days=published_days,
        artifact_temporary_retention_hours=temporary_hours,
    )
    return ComputeOperationsService(get_connection(), clock, settings)


def _completed_task(service: ComputeOperationsService, key: str, data: bytes = LOG) -> tuple[int, int, list[dict]]:
    task = service.submit(submit_payload(key))
    claimed = service.claim("w1", ["solver-a"], 300)
    from app.compute.schemas import ArtifactManifestItem
    items = [ArtifactManifestItem(**artifact_item(filename="run.log", data=data, purpose="运行日志", worker_path="/work/run.log"))]
    completed = service.complete(claimed["id"], "w1", {"score": 90}, {}, items, receipt_key=f"receipt-{key}")
    return claimed["id"], completed["result_version"], completed["artifacts"]


def test_purge_keeps_current_candidate_and_published_removes_withdrawn(client, tmp_path):
    clock = FrozenClock(datetime(2026, 9, 29, 0, 0, tzinfo=UTC))
    store = tmp_path / "artifacts"
    service = _service_with_clock(clock, tmp_store=store, candidate_days=30, withdrawn_days=30)
    service.create_template(TEMPLATE, "administrator")

    candidate_data, published_data, withdrawn_data = b"candidate-body", b"published-body", b"withdrawn-body"
    candidate_task, _, candidate_artifacts = _completed_task(service, "retention-candidate", candidate_data)
    published_task, published_version, published_artifacts = _completed_task(service, "retention-published", published_data)
    withdrawn_task, withdrawn_version, withdrawn_artifacts = _completed_task(service, "retention-withdrawn", withdrawn_data)
    service.publish_result(published_task, published_version, "administrator", "公布")
    service.publish_result(withdrawn_task, withdrawn_version, "administrator", "公布")
    service.withdraw_result(withdrawn_task, withdrawn_version, "administrator", "复核撤回")

    candidate_path = store / service._relpath_for_sha(candidate_artifacts[0]["blob_sha256"])
    published_path = store / service._relpath_for_sha(published_artifacts[0]["blob_sha256"])
    withdrawn_path = store / service._relpath_for_sha(withdrawn_artifacts[0]["blob_sha256"])
    assert candidate_path.is_file() and published_path.is_file() and withdrawn_path.is_file()

    clock.advance(days=31)
    report = service.purge_expired_artifacts()
    purged_ids = {item["artifact_id"] for item in report["artifacts_purged"]}
    # 候选版本仍是当前成绩 → 保留；发布 → 保留；撤回且过期 → 清理
    assert candidate_artifacts[0]["id"] not in purged_ids
    assert published_artifacts[0]["id"] not in purged_ids
    assert withdrawn_artifacts[0]["id"] in purged_ids
    # 过期但被引用而保留的只有“当前候选版本”；发布版本保留期 3650 天，尚未过期故不计入
    assert report["referenced_artifacts_retained"] == 1
    assert candidate_path.is_file() and published_path.is_file()
    assert not withdrawn_path.exists()

    # 已发布内容即使超过候选/撤回保留期也仍可下载
    decision = service.explain_artifact_access(published_artifacts[0]["id"], "obs-pub")
    # 无项目授权时观察员仍被拒，换管理员确认保留期不阻挡已发布内容
    admin = service.explain_artifact_access(published_artifacts[0]["id"], "administrator")
    assert admin["allowed"] is True
    assert decision["decision_code"] == "permission_denied"

    # dry_run 不产生变更
    dry = service.purge_expired_artifacts(dry_run=True)
    assert dry["artifacts_purged"] == []


def test_candidate_current_version_protected_past_retention(client, tmp_path):
    clock = FrozenClock(datetime(2026, 9, 29, tzinfo=UTC))
    service = _service_with_clock(clock, tmp_store=tmp_path / "artifacts", candidate_days=30)
    store = tmp_path / "artifacts"
    service.create_template(TEMPLATE, "administrator")
    task_id, version, artifacts = _completed_task(service, "retention-current-only")
    clock.advance(days=100)
    report = service.purge_expired_artifacts()
    assert report["artifacts_purged"] == []
    assert report["referenced_artifacts_retained"] == 1
    # 物理内容仍在（被当前成绩引用），但已过保留期，下载按规格拒绝
    physical = store / service._relpath_for_sha(artifacts[0]["blob_sha256"])
    assert physical.is_file()
    from app.core.errors import PermissionDeniedError
    try:
        service.download_artifact(artifacts[0]["id"], "researcher-1")
    except PermissionDeniedError as exc:
        assert exc.context["decision_code"] == "retention_expired"
    else:
        raise AssertionError("过保留期的候选成果应当拒绝下载")
    # 管理员走说明接口也显示拒绝，且成绩版本归属清晰
    decision = service.explain_artifact_access(artifacts[0]["id"], "researcher-1")
    assert decision["allowed"] is False
    assert decision["result_version"] == version
    assert decision["is_current_version"] is True


def test_temporary_staging_expires_and_blob_deleted(client, tmp_path):
    clock = FrozenClock(datetime(2026, 9, 29, tzinfo=UTC))
    store = tmp_path / "artifacts"
    service = _service_with_clock(clock, tmp_store=store)
    from app.compute.schemas import ArtifactStaging
    payload = ArtifactStaging(
        staging_key="temp-stage-1", worker_id="w1", filename="scratch.tmp", purpose="临时中间文件",
        size_bytes=len(SIM), content_sha256=sha(SIM), content_base64=b64(SIM), ttl_seconds=60,
    )
    staged = service.stage_artifact(payload)
    physical = store / service._relpath_for_sha(staged["blob_sha256"])
    assert physical.is_file()
    clock.advance(seconds=61)
    report = service.purge_expired_artifacts()
    assert report["temporary_staging_removed"] == ["temp-stage-1"]
    assert staged["blob_sha256"] in report["blobs_deleted"]
    assert not physical.exists()


def test_purged_artifact_download_returns_gone(client, tmp_path):
    clock = FrozenClock(datetime(2026, 9, 29, tzinfo=UTC))
    service = _service_with_clock(clock, tmp_store=tmp_path / "artifacts", withdrawn_days=1)
    service.create_template(TEMPLATE, "administrator")
    task_id, version, artifacts = _completed_task(service, "retention-gone", b"gone-body")
    service.publish_result(task_id, version, "administrator", "公布")
    service.withdraw_result(task_id, version, "administrator", "撤回")
    clock.advance(days=2)
    service.purge_expired_artifacts()
    from app.core.errors import GoneError
    try:
        service.download_artifact(artifacts[0]["id"], "administrator")
    except GoneError as exc:
        assert exc.status_code == 410
    else:
        raise AssertionError("已清理的成果应当返回 410")
