from __future__ import annotations

from fastapi import APIRouter, Query, Request
from fastapi.responses import FileResponse

from app.compute.schemas import (
    BatchOperation,
    CancelRequest,
    PriorityRequest,
    ProjectMemberGrant,
    PublishResultRequest,
    QuotaSet,
    RetryRequest,
    TaskClaim,
    TaskFailure,
    TaskResult,
    TaskSubmit,
    TemplateCreate,
    WithdrawResultRequest,
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


@router.put("/artifacts/uploads/{upload_id}")
async def upload_artifact(
    upload_id: str,
    request: Request,
    worker_id: str = Query(..., min_length=1, max_length=120),
    filename: str = Query(..., min_length=1, max_length=200),
    purpose: str = Query(..., min_length=1, max_length=200),
):
    """工作者上传单个成果文件到受控暂存区，服务端实测大小与摘要。"""
    return await service().register_artifact_upload(
        upload_id, worker_id, filename, purpose, request.stream()
    )


@router.post("/project-members", status_code=201)
def grant_project_member(payload: ProjectMemberGrant):
    return service().grant_project_member(payload.model_dump())


@router.post("/tasks/{task_id}/complete")
def complete_task(task_id: int, payload: TaskResult):
    data = payload.model_dump()
    return service().complete(
        task_id,
        data["worker_id"],
        data["result"],
        data["metrics"],
        data.get("artifacts") or [],
        data.get("receipt_key"),
    )


@router.post("/tasks/{task_id}/fail")
def fail_task(task_id: int, payload: TaskFailure):
    return service().fail(task_id, payload.worker_id, payload.error_code, payload.message, payload.retryable)


@router.post("/tasks/{task_id}/results/{version}/publish")
def publish_result(task_id: int, version: int, payload: PublishResultRequest):
    return service().publish_result(task_id, version, payload.actor, payload.reason)


@router.post("/tasks/{task_id}/results/{version}/withdraw")
def withdraw_result(task_id: int, version: int, payload: WithdrawResultRequest):
    return service().withdraw_result(task_id, version, payload.actor, payload.reason)


@router.get("/tasks/{task_id}/results/{version}/artifacts/{filename}/access")
def artifact_access(task_id: int, version: int, filename: str, requested_by: str = Query(..., min_length=1)):
    """说明文件属于哪个成绩版本，以及为何允许或拒绝下载（不返回内容）。"""
    return service().artifact_access(task_id, version, filename, requested_by)


@router.get("/artifacts/{artifact_id}/access")
def artifact_access_by_id(artifact_id: int, requested_by: str = Query(..., min_length=1)):
    return service().artifact_access_by_id(artifact_id, requested_by)


@router.get("/tasks/{task_id}/results/{version}/artifacts/{filename}/download")
def download_artifact(task_id: int, version: int, filename: str, requested_by: str = Query(..., min_length=1)):
    """只允许访问仍在保留期且有权限的版本对应的物理文件。"""
    path, artifact, reasons = service().open_artifact_download(task_id, version, filename, requested_by)
    del reasons  # 授权原因由 access 接口说明，下载响应头只携带 ASCII 安全的版本信息。
    return FileResponse(
        path,
        filename=artifact["filename"],
        headers={"X-Result-Version": f"{artifact['task_id']}-{artifact['result_version']}", "X-Artifact-State": artifact["state"]},
    )


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


@router.post("/recovery/expired-leases")
def recover_expired(actor: str = Query(default="recovery-worker", min_length=1)):
    return service().recover_expired(actor)


@router.post("/retention/cleanup-artifacts")
def cleanup_artifacts(actor: str = Query(default="retention-worker", min_length=1)):
    return service().cleanup_artifacts(actor)


@router.get("/summary")
def summary():
    return service().summary()
