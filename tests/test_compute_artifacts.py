from __future__ import annotations

import hashlib
from datetime import UTC, datetime

import pytest

from app.compute.service import ComputeOperationsService
from app.core.clock import FrozenClock
from app.database import get_connection

from tests.test_compute_operations import TEMPLATE, create_template, submit_payload


@pytest.fixture(autouse=True)
def _template(client):
    create_template(client)


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _setup_task(client, worker="w1", key="artifact-000001", capabilities=None):
    task = client.post("/api/compute/tasks", json=submit_payload(key)).json()
    claimed = client.post(
        "/api/compute/tasks/claim",
        json={"worker_id": worker, "capabilities": capabilities or ["solver-a"], "lease_seconds": 60},
    ).json()["task"]
    return task


def _upload(client, upload_id, data, *, worker="w1", filename="run.log", purpose="运行日志"):
    response = client.put(
        f"/api/compute/artifacts/uploads/{upload_id}",
        params={"worker_id": worker, "filename": filename, "purpose": purpose},
        content=data,
    )
    assert response.status_code == 200, response.text
    return response.json()


def _freeze_service(days=0, hours=0) -> ComputeOperationsService:
    clock = FrozenClock(datetime.now(UTC))
    clock.advance(days=days, hours=hours)
    return ComputeOperationsService(get_connection(), clock)


def test_manifest_verified_and_persisted_with_result_version(client):
    task = _setup_task(client)
    log = b"log line one\nlog line two\n"
    uploaded = _upload(client, "upload-log-1", log)
    assert uploaded["size_bytes"] == len(log)
    assert uploaded["sha256"] == _sha(log)

    complete = client.post(
        f"/api/compute/tasks/{task['id']}/complete",
        json={
            "worker_id": "w1",
            "result": {"score": 92},
            "metrics": {"seconds": 3},
            "receipt_key": "receipt-0001",
            "artifacts": [
                {
                    "upload_id": uploaded["upload_id"],
                    "path": uploaded["path"],
                    "filename": uploaded["filename"],
                    "purpose": uploaded["purpose"],
                    "size_bytes": uploaded["size_bytes"],
                    "sha256": uploaded["sha256"],
                }
            ],
        },
    )
    assert complete.status_code == 200, complete.text
    body = complete.json()
    assert body["status"] == "succeeded"
    assert body["current_result_version"] == 1
    artifact = body["artifacts"][0]
    assert artifact["result_version"] == 1
    assert artifact["state"] == "candidate"
    assert artifact["sha256"] == _sha(log)
    assert artifact["retention_until"] > body["finished_at"]


def test_manifest_path_mismatch_and_traversal_rejected(client):
    task = _setup_task(client, key="artifact-path-check")
    uploaded = _upload(client, "upload-path-1", b"path bytes")
    base = {
        "upload_id": uploaded["upload_id"],
        "path": uploaded["path"],
        "filename": uploaded["filename"],
        "purpose": uploaded["purpose"],
        "size_bytes": uploaded["size_bytes"],
        "sha256": uploaded["sha256"],
    }

    tampered = dict(base, path="staging/someone-else")
    resp = client.post(f"/api/compute/tasks/{task['id']}/complete", json={"worker_id": "w1", "result": {}, "metrics": {}, "artifacts": [tampered]})
    assert resp.status_code == 422
    assert client.get(f"/api/compute/task-details/{task['id']}").json()["status"] == "running"

    # 路径穿越必须被拒绝（任务仍 running，可重试）。
    traversal = dict(base, path="../../etc/passwd")
    resp2 = client.post(f"/api/compute/tasks/{task['id']}/complete", json={"worker_id": "w1", "result": {}, "metrics": {}, "artifacts": [traversal]})
    assert resp2.status_code == 422


def test_digest_mismatch_blocks_success_and_persists_nothing(client):
    task = _setup_task(client, key="artifact-bad-digest")
    uploaded = _upload(client, "upload-bad-1", b"actual bytes")
    bad_sha = ("0" if uploaded["sha256"][0] != "0" else "1") + uploaded["sha256"][1:]

    response = client.post(
        f"/api/compute/tasks/{task['id']}/complete",
        json={
            "worker_id": "w1",
            "result": {"score": 1},
            "metrics": {},
            "artifacts": [
                {
                    "upload_id": uploaded["upload_id"],
                    "path": uploaded["path"],
                    "filename": uploaded["filename"],
                    "purpose": uploaded["purpose"],
                    "size_bytes": uploaded["size_bytes"],
                    "sha256": bad_sha,
                }
            ],
        },
    )
    assert response.status_code == 422
    details = client.get(f"/api/compute/task-details/{task['id']}").json()
    # 摘要不符：任务不能转为成功，且没有任何成绩版本或清单落库。
    assert details["status"] == "running"
    assert details["results"] == []
    assert details["artifacts"] == []


def test_duplicate_receipt_produces_single_manifest(client):
    task = _setup_task(client, key="artifact-idempotent")
    uploaded = _upload(client, "upload-idem-1", b"hello")
    payload = {
        "worker_id": "w1",
        "result": {"score": 80},
        "metrics": {},
        "receipt_key": "receipt-dup-1",
        "artifacts": [
            {
                "upload_id": uploaded["upload_id"],
                "path": uploaded["path"],
                "filename": uploaded["filename"],
                "purpose": uploaded["purpose"],
                "size_bytes": uploaded["size_bytes"],
                "sha256": uploaded["sha256"],
            }
        ],
    }
    first = client.post(f"/api/compute/tasks/{task['id']}/complete", json=payload)
    assert first.status_code == 200
    # 任务已成功后再次重放同一回执：仍返回同一版本，不产生第二份清单。
    second = client.post(f"/api/compute/tasks/{task['id']}/complete", json=payload)
    assert second.status_code == 200
    details = client.get(f"/api/compute/task-details/{task['id']}").json()
    assert len(details["results"]) == 1
    assert len(details["artifacts"]) == 1
    assert details["current_result_version"] == 1


def test_cannot_reference_other_worker_upload(client):
    task = _setup_task(client, key="artifact-foreign")
    uploaded = _upload(client, "upload-foreign-1", b"data", worker="other-worker")
    response = client.post(
        f"/api/compute/tasks/{task['id']}/complete",
        json={
            "worker_id": "w1",
            "result": {},
            "metrics": {},
            "artifacts": [
                {
                    "upload_id": uploaded["upload_id"],
                    "path": uploaded["path"],
                    "filename": uploaded["filename"],
                    "purpose": uploaded["purpose"],
                    "size_bytes": uploaded["size_bytes"],
                    "sha256": uploaded["sha256"],
                }
            ],
        },
    )
    assert response.status_code == 403
    assert client.get(f"/api/compute/task-details/{task['id']}").json()["status"] == "running"


def _publish_flow(client, *, key="artifact-publish"):
    task = _setup_task(client, key=key)
    uploaded = _upload(client, f"upload-{key}", b"simulation-binary")
    client.post(
        f"/api/compute/tasks/{task['id']}/complete",
        json={
            "worker_id": "w1",
            "result": {"grade": "A"},
            "metrics": {},
            "artifacts": [
                {
                    "upload_id": uploaded["upload_id"],
                    "path": uploaded["path"],
                    "filename": uploaded["filename"],
                    "purpose": uploaded["purpose"],
                    "size_bytes": uploaded["size_bytes"],
                    "sha256": uploaded["sha256"],
                }
            ],
        },
    )
    client.post(
        "/api/compute/project-members",
        json={"project_code": "project-a", "member": "teacher-li", "role": "teacher", "granted_by": "admin"},
    )
    client.post(
        "/api/compute/project-members",
        json={"project_code": "project-a", "member": "admin-wang", "role": "admin", "granted_by": "admin"},
    )
    published = client.post(
        f"/api/compute/tasks/{task['id']}/results/1/publish",
        json={"actor": "admin-wang", "reason": "成绩公示"},
    )
    assert published.status_code == 200
    assert published.json()["lifecycle_state"] == "published"
    return task


def test_access_explains_version_and_download_permissions(client):
    task = _publish_flow(client)
    url = f"/api/compute/tasks/{task['id']}/results/1/artifacts/run.log"

    # 任课教师：允许，且接口说明所属成绩版本与原因。
    teacher = client.get(url + "/access", params={"requested_by": "teacher-li"})
    assert teacher.status_code == 200
    view = teacher.json()
    assert view["allowed"] is True
    assert view["result_version"] == {"task_id": task["id"], "version": 1, "state": "published"}
    assert view["referenced_by_public_grade"] is True
    assert any("教师" in r for r in view["reasons"])

    download = client.get(url + "/download", params={"requested_by": "teacher-li"})
    assert download.status_code == 200
    assert download.content == b"simulation-binary"
    assert download.headers["x-result-version"] == f"{task['id']}-1"

    # 提交学生本人：已发布成果不直接对学生开放。
    student = client.get(url + "/access", params={"requested_by": "researcher-1"})
    assert student.json()["allowed"] is False
    assert client.get(url + "/download", params={"requested_by": "researcher-1"}).status_code == 403

    # 无关人员：拒绝。
    outsider = client.get(url + "/download", params={"requested_by": "nobody"})
    assert outsider.status_code == 403


def test_withdrawn_only_admin_auditable_within_retention(client):
    task = _publish_flow(client, key="artifact-withdraw")
    withdraw = client.post(
        f"/api/compute/tasks/{task['id']}/results/1/withdraw",
        json={"actor": "admin-wang", "reason": "成绩复核暂撤回"},
    )
    assert withdraw.status_code == 200
    assert withdraw.json()["lifecycle_state"] == "withdrawn"
    url = f"/api/compute/tasks/{task['id']}/results/1/artifacts/run.log"

    assert client.get(url + "/download", params={"requested_by": "teacher-li"}).status_code == 403
    admin = client.get(url + "/download", params={"requested_by": "admin-wang"})
    assert admin.status_code == 200
    assert admin.content == b"simulation-binary"


def test_cleanup_rules_for_temp_candidate_withdrawn_and_published(client):
    # 已发布并被公开成绩引用的成果：必须长期保留。
    published_task = _publish_flow(client, key="artifact-retain-pub")

    # 候选版本：超过候选保留期可清理。
    candidate_task = _setup_task(client, worker="w2", key="artifact-candidate-expire")
    cand_upload = _upload(client, "upload-candidate-exp", b"candidate bytes", worker="w2")
    client.post(
        f"/api/compute/tasks/{candidate_task['id']}/complete",
        json={
            "worker_id": "w2",
            "result": {"g": 1},
            "metrics": {},
            "artifacts": [
                {
                    "upload_id": cand_upload["upload_id"],
                    "path": cand_upload["path"],
                    "filename": cand_upload["filename"],
                    "purpose": cand_upload["purpose"],
                    "size_bytes": cand_upload["size_bytes"],
                    "sha256": cand_upload["sha256"],
                }
            ],
        },
    )

    # 撤回版本：发布后撤回，超过撤回留存期可清理。
    withdrawn_task = _publish_flow(client, key="artifact-withdraw-expire")
    client.post(
        f"/api/compute/tasks/{withdrawn_task['id']}/results/1/withdraw",
        json={"actor": "admin-wang", "reason": "复核撤回"},
    )

    # 临时文件：上传后从不回执，超过暂存保留期可清理。
    _upload(client, "upload-temp-exp", b"orphan temp", worker="w2")

    # 远超过候选(7天)/撤回(30天)/暂存(24小时)保留期，但仍短于/无关发布保护。
    report = _freeze_service(days=40).cleanup_artifacts()
    assert "upload-temp-exp" in report["removed_temp_uploads"]
    assert len(report["removed_candidate_artifacts"]) == 1
    assert len(report["removed_withdrawn_artifacts"]) == 1
    assert report["protected_published_artifacts"] >= 1

    # 被公开成绩引用的已发布成果仍然存在且可下载。
    details = client.get(f"/api/compute/task-details/{published_task['id']}").json()
    assert len(details["artifacts"]) == 1
    download = client.get(
        f"/api/compute/tasks/{published_task['id']}/results/1/artifacts/run.log/download",
        params={"requested_by": "teacher-li"},
    )
    assert download.status_code == 200
    assert download.content == b"simulation-binary"

    # 候选、撤回成果已不可访问（404）。
    gone = client.get(
        f"/api/compute/tasks/{candidate_task['id']}/results/1/artifacts/run.log/access",
        params={"requested_by": "teacher-li"},
    )
    assert gone.status_code == 404


def test_publishing_new_version_supersedes_previous_and_keeps_current(client):
    # v1 完成并发布。
    task = _setup_task(client, key="artifact-supersede")
    up1 = _upload(client, "upload-super-1", b"version-one")
    client.post(
        f"/api/compute/tasks/{task['id']}/complete",
        json={
            "worker_id": "w1", "result": {"g": 1}, "metrics": {},
            "artifacts": [{
                "upload_id": up1["upload_id"], "path": up1["path"], "filename": up1["filename"],
                "purpose": up1["purpose"], "size_bytes": up1["size_bytes"], "sha256": up1["sha256"],
            }],
        },
    )
    client.post(
        "/api/compute/project-members",
        json={"project_code": "project-a", "member": "admin-wang", "role": "admin", "granted_by": "admin"},
    )
    assert client.post(f"/api/compute/tasks/{task['id']}/results/1/publish", json={"actor": "admin-wang", "reason": "首次发布"}).status_code == 200

    # 任务重试后产生 v2 候选，再发布：v1 应被级联为“被替代撤回”，公开引用切到 v2。
    assert client.post(f"/api/compute/tasks/{task['id']}/retry", json={"actor": "admin-wang", "reason": "复算"}).status_code == 200
    claim = client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 60}).json()["task"]
    assert claim["id"] == task["id"]
    up2 = _upload(client, "upload-super-2", b"version-two")
    client.post(
        f"/api/compute/tasks/{task['id']}/complete",
        json={
            "worker_id": "w1", "result": {"g": 2}, "metrics": {},
            "artifacts": [{
                "upload_id": up2["upload_id"], "path": up2["path"], "filename": up2["filename"],
                "purpose": up2["purpose"], "size_bytes": up2["size_bytes"], "sha256": up2["sha256"],
            }],
        },
    )
    client.post(f"/api/compute/tasks/{task['id']}/results/2/publish", json={"actor": "admin-wang", "reason": "更新发布"})

    details = client.get(f"/api/compute/task-details/{task['id']}").json()
    states = {r["version"]: r["lifecycle_state"] for r in details["results"]}
    assert states == {1: "withdrawn", 2: "published"}
    artifact_states = {a["result_version"]: a["state"] for a in details["artifacts"]}
    assert artifact_states == {1: "withdrawn", 2: "published"}

    # 超过撤回留存期清理：v1 被删除，v2 作为当前公开成绩永久保留。
    report = _freeze_service(days=40).cleanup_artifacts()
    assert len(report["removed_withdrawn_artifacts"]) == 1
    after = client.get(f"/api/compute/task-details/{task['id']}").json()
    remaining = {a["result_version"] for a in after["artifacts"]}
    assert remaining == {2}
    download = client.get(
        f"/api/compute/tasks/{task['id']}/results/2/artifacts/run.log/download",
        params={"requested_by": "admin-wang"},
    )
    assert download.status_code == 200
    assert download.content == b"version-two"


def test_published_referenced_content_survives_far_future_cleanup(client):
    task = _publish_flow(client, key="artifact-forever")
    # 即使远超发布保留期（默认 3650 天），被当前公开成绩引用也不得删除。
    _freeze_service(days=4000).cleanup_artifacts()
    access = client.get(
        f"/api/compute/tasks/{task['id']}/results/1/artifacts/run.log/access",
        params={"requested_by": "teacher-li"},
    )
    assert access.status_code == 200
    assert access.json()["allowed"] is True
