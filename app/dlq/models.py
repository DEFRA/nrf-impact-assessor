"""Pydantic request/response models for the DLQ admin API."""

from datetime import datetime

from pydantic import BaseModel


class DlqStats(BaseModel):
    available: int
    in_flight: int


class DlqMessage(BaseModel):
    message_id: str
    receipt_handle: str
    body: str  # full, untruncated — echoed back verbatim on redrive
    body_preview: str  # display-only, <= body_preview_limit
    body_truncated: bool
    body_bytes: int
    receive_count: int
    sent_at: datetime | None = None


class DlqPeekResult(BaseModel):
    messages: list[DlqMessage]
    visibility_deadline: datetime
    hold_seconds: int


class RedriveAllRequest(BaseModel):
    confirm: bool = False
    max_per_second: int | None = None


class RedriveMessageRequest(BaseModel):
    receipt_handle: str
    body: str  # the full body received from peek


class RedriveTask(BaseModel):
    task_handle: str | None = None
    status: str | None = None
    approx_messages_moved: int | None = None
    source_arn: str | None = None
    started_at: datetime | None = None
