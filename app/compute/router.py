from __future__ import annotations

from fastapi import APIRouter, Query
from fastapi.responses import Response

from app.compute.schemas import (
    ArtifactStaging,
    BatchOperation,
    CancelRequest,
    PriorityRequest,
    ProjectMemberGrant,
    PublishRequest,
    QuotaSet,
    RetryRequest,
    TaskClaim,
    TaskFailure,
    TaskResult,
    TaskSubmit,
    TemplateCreate,
    WithdrawRequest,
)
from app.compute.service import ComputeOperationsService

router = APIRouter(prefix="/api/compute", tags=["科学计算任务运营"])


def service() -> ComputeOperationsService:
    return ComputeOperationsService()


@router.get("/templates")
def list_templates():
    return {"items": service().list_templates()}


@router.post("/templates", status_code=201)
def create_template(payload: TemplateCreate, actor: str = Query(..., min_length=1)):
    return service().create_template(payload.model_dump(), actor)


@router.put("/quotas")
def set_quota(payload: QuotaSet, actor: str = Query(..., min_length=1)):
    return service().set_quota(payload.model_dump(), actor)


@router.post("/tasks", status_code=202)
def submit_task(payload: TaskSubmit):
    return service().submit(payload.model_dump())


@router.get("/tasks")
def list_tasks(status: str | None = None, project_code: str | None = None, requested_by: str | None = None, limit: int = Query(default=100, ge=1, le=500)):
    return {"items": service().list_tasks(status=status, project_code=project_code, requested_by=requested_by, limit=limit)}


@router.get("/task-details/{task_id}")
def get_task(task_id: int):
    return service().get_task(task_id)


@router.post("/tasks/claim")
def claim_task(payload: TaskClaim):
    return {"task": service().claim(payload.worker_id, payload.capabilities, payload.lease_seconds)}


@router.post("/tasks/{task_id}/heartbeat")
def heartbeat(task_id: int, payload: TaskClaim):
    return service().heartbeat(task_id, payload.worker_id, payload.lease_seconds)


@router.post("/tasks/{task_id}/complete")
def complete_task(task_id: int, payload: TaskResult):
    return service().complete(
        task_id, payload.worker_id, payload.result, payload.metrics,
        artifacts=payload.artifacts, receipt_key=payload.receipt_key,
    )


@router.post("/tasks/{task_id}/fail")
def fail_task(task_id: int, payload: TaskFailure):
    return service().fail(task_id, payload.worker_id, payload.error_code, payload.message, payload.retryable)


@router.post("/tasks/{task_id}/cancel")
def cancel_task(task_id: int, payload: CancelRequest):
    return service().cancel(task_id, payload.actor, payload.reason)


@router.post("/tasks/{task_id}/retry")
def retry_task(task_id: int, payload: RetryRequest):
    return service().retry(task_id, payload.actor, payload.reason, payload.priority)


@router.post("/tasks/{task_id}/priority")
def set_priority(task_id: int, payload: PriorityRequest):
    return service().set_priority(task_id, payload.actor, payload.reason, payload.priority)


@router.post("/tasks/batch")
def batch_operation(payload: BatchOperation):
    return service().batch_operation(payload.model_dump())


@router.post("/tasks/{task_id}/results/{version}/publish")
def publish_result(task_id: int, version: int, payload: PublishRequest):
    return service().publish_result(task_id, version, payload.actor, payload.reason)


@router.post("/tasks/{task_id}/results/{version}/withdraw")
def withdraw_result(task_id: int, version: int, payload: WithdrawRequest):
    return service().withdraw_result(task_id, version, payload.actor, payload.reason)


@router.put("/project-members")
def grant_project_member(payload: ProjectMemberGrant):
    return service().grant_project_member(payload.model_dump())


@router.post("/artifacts/staging", status_code=201)
def stage_artifact(payload: ArtifactStaging):
    return service().stage_artifact(payload)


@router.get("/artifacts/{artifact_id}/access")
def explain_artifact_access(artifact_id: int, requester: str = Query(..., min_length=1)):
    """说明成果文件属于哪个成绩版本，以及当前请求者为何被允许或拒绝下载。"""
    return service().explain_artifact_access(artifact_id, requester)


@router.get("/artifacts/{artifact_id}/download")
def download_artifact(artifact_id: int, requester: str = Query(..., min_length=1)):
    data, artifact, decision = service().download_artifact(artifact_id, requester)
    quoted_filename = artifact["filename"].replace('"', "")
    return Response(
        content=data,
        media_type=artifact["content_type"] or "application/octet-stream",
        headers={
            "Content-Disposition": f'attachment; filename="{quoted_filename}"',
            "X-Artifact-Id": str(artifact["id"]),
            "X-Result-Version": str(artifact["result_version"]),
            "X-Access-Decision": decision["decision_code"],
        },
    )


@router.post("/retention/purge-artifacts")
def purge_expired_artifacts(actor: str = Query(default="retention-worker", min_length=1), dry_run: bool = Query(default=False)):
    return service().purge_expired_artifacts(actor, dry_run=dry_run)


@router.post("/recovery/expired-leases")
def recover_expired(actor: str = Query(default="recovery-worker", min_length=1)):
    return service().recover_expired(actor)


@router.get("/summary")
def summary():
    return service().summary()
