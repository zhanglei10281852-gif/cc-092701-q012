from __future__ import annotations

import base64
import binascii
import re
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

_SHA256_PATTERN = re.compile(r"^[a-f0-9]{64}$")
_FILENAME_PATTERN = re.compile(r"^[^/\x00]+$")


def _validate_sha256(value: str) -> str:
    normalized = value.strip().lower()
    if not _SHA256_PATTERN.fullmatch(normalized):
        raise ValueError("content_sha256 必须是 64 位十六进制摘要")
    return normalized


def _validate_filename(value: str) -> str:
    value = value.strip()
    if not value or not _FILENAME_PATTERN.fullmatch(value) or value in {".", ".."}:
        raise ValueError("filename 必须是不含路径分隔符的文件名")
    return value


class ArtifactManifestItem(BaseModel):
    """工作者回执中声明的单个成果文件清单条目。"""

    worker_path: str = Field(min_length=1, max_length=500, description="工作者侧的提交路径")
    filename: str = Field(min_length=1, max_length=200, description="落库文件名，不含路径分隔符")
    size_bytes: int = Field(ge=0, le=512 * 1024 * 1024)
    content_sha256: str = Field(description="工作者计算的文件 sha256 摘要")
    purpose: str = Field(min_length=1, max_length=200, description="用途，例如 仿真日志 / 结果数据")
    content_type: str = Field(default="application/octet-stream", max_length=120)
    content_base64: str | None = Field(default=None, description="内联文件内容（base64）；与 staging_key 二选一")
    staging_key: str | None = Field(default=None, max_length=120, description="预先暂存返回的临时键；与 content_base64 二选一")

    @model_validator(mode="after")
    def _normalize(self) -> "ArtifactManifestItem":
        self.content_sha256 = _validate_sha256(self.content_sha256)
        self.filename = _validate_filename(self.filename)
        if (self.content_base64 is None) == (self.staging_key is None):
            raise ValueError("每个清单条目必须通过 content_base64 或 staging_key 之一提供内容")
        return self

    def decode_content(self) -> bytes:
        if self.content_base64 is None:
            raise ValueError("该清单条目引用暂存区，没有内联内容")
        try:
            return base64.b64decode(self.content_base64, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError("content_base64 不是合法的 base64 内容") from exc


class ArtifactStaging(BaseModel):
    staging_key: str = Field(min_length=8, max_length=120)
    worker_id: str = Field(min_length=1, max_length=120)
    filename: str = Field(min_length=1, max_length=200)
    purpose: str = Field(min_length=1, max_length=200)
    size_bytes: int = Field(ge=0, le=512 * 1024 * 1024)
    content_sha256: str
    content_type: str = Field(default="application/octet-stream", max_length=120)
    content_base64: str
    ttl_seconds: int = Field(default=86400, ge=60, le=7 * 86400)

    @model_validator(mode="after")
    def _normalize(self) -> "ArtifactStaging":
        self.content_sha256 = _validate_sha256(self.content_sha256)
        self.filename = _validate_filename(self.filename)
        return self

    def decode_content(self) -> bytes:
        try:
            return base64.b64decode(self.content_base64, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError("content_base64 不是合法的 base64 内容") from exc


class TaskResult(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
    result: dict[str, Any]
    metrics: dict[str, Any] = Field(default_factory=dict)
    receipt_key: str | None = Field(default=None, min_length=6, max_length=160, description="工作者回执幂等键；携带成果清单时必填")
    artifacts: list[ArtifactManifestItem] = Field(default_factory=list, max_length=100)

    @model_validator(mode="after")
    def _require_receipt_for_artifacts(self) -> "TaskResult":
        if self.artifacts and not self.receipt_key:
            raise ValueError("携带成果清单的回执必须提供 receipt_key，以保证重复回执只产生一份清单")
        names = [item.filename for item in self.artifacts]
        if len(set(names)) != len(names):
            raise ValueError("成果清单中的 filename 不能重复")
        return self


class PublishRequest(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(default="成绩发布", min_length=1, max_length=1000)


class WithdrawRequest(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=1000)


class ProjectMemberGrant(BaseModel):
    project_code: str = Field(min_length=1, max_length=80)
    member: str = Field(min_length=1, max_length=120, description="可下载成果的教师或观察员标识")
    member_role: Literal["teacher", "observer"]
    actor: str = Field(min_length=1, max_length=120)


class TemplateCreate(BaseModel):
    code: str = Field(min_length=2, max_length=64, pattern=r"^[a-z0-9][a-z0-9._-]+$")
    name: str = Field(min_length=2, max_length=120)
    algorithm: str = Field(min_length=2, max_length=120)
    parameter_schema: dict[str, dict[str, Any]]
    default_parameters: dict[str, Any] = Field(default_factory=dict)
    max_runtime_seconds: int = Field(default=600, ge=1, le=86400)
    max_attempts: int = Field(default=3, ge=1, le=20)


class QuotaSet(BaseModel):
    subject_type: Literal["user", "role", "project"]
    subject_key: str = Field(min_length=1, max_length=120)
    max_queued: int = Field(default=20, ge=0, le=100000)
    max_running: int = Field(default=4, ge=0, le=10000)
    daily_submissions: int = Field(default=200, ge=0, le=1000000)


class TaskSubmit(BaseModel):
    template_code: str = Field(min_length=2, max_length=64)
    project_code: str = Field(min_length=1, max_length=80)
    requested_by: str = Field(min_length=1, max_length=80)
    parameters: dict[str, Any]
    priority: int = Field(default=50, ge=0, le=100)
    idempotency_key: str = Field(min_length=6, max_length=160)


class TaskClaim(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
    capabilities: list[str] = Field(default_factory=list, max_length=100)
    lease_seconds: int = Field(default=60, ge=5, le=3600)


class TaskFailure(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
    error_code: str = Field(min_length=1, max_length=120)
    message: str = Field(min_length=1, max_length=2000)
    retryable: bool = True


class CancelRequest(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=1000)


class RetryRequest(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=1000)
    priority: int | None = Field(default=None, ge=0, le=100)


class PriorityRequest(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=1000)
    priority: int = Field(ge=0, le=100)


class BatchOperation(BaseModel):
    task_ids: list[int] = Field(min_length=1, max_length=200)
    operation: Literal["cancel", "retry", "priority"]
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=1000)
    priority: int | None = Field(default=None, ge=0, le=100)

    @model_validator(mode="after")
    def validate_priority(self) -> "BatchOperation":
        if self.operation == "priority" and self.priority is None:
            raise ValueError("批量调整优先级时必须提供 priority")
        return self
