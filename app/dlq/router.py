"""Token-authenticated admin endpoints to inspect and redrive the DLQ."""

import hashlib
import logging
from collections.abc import Callable

from fastapi import APIRouter, Depends, Header, HTTPException, Query

from app.common import metrics
from app.common.tracing import ctx_trace_id
from app.config import AWSConfig, DlqAdminConfig
from app.dlq.models import (
    DlqPeekResult,
    DlqStats,
    RedriveAllRequest,
    RedriveMessageRequest,
    RedriveTask,
)
from app.dlq.service import DlqError, DlqService

logger = logging.getLogger(__name__)

router = APIRouter()

_service: DlqService | None = None

_HTTP_BY_CODE = {
    "handle_expired": 409,
    "invalid_parameter": 400,
    "dlq_unavailable": 503,
    "sqs_error": 502,
}


def _get_service() -> DlqService:
    global _service
    if _service is None:
        aws = AWSConfig()
        cfg = DlqAdminConfig()
        _service = DlqService(
            dlq_url=aws.sqs_dlq_url,
            main_queue_url=aws.sqs_queue_url,
            region=aws.region,
            endpoint_url=aws.endpoint_url,
            body_preview_limit=cfg.body_preview_limit,
        )
    return _service


def dlq_auth(x_dlq_token: str | None = Header(default=None)) -> str:
    """Validate the DLQ admin token; return a short non-reversible fingerprint."""
    cfg = DlqAdminConfig()
    if not cfg.auth_token or x_dlq_token != cfg.auth_token:
        raise HTTPException(
            status_code=401,
            detail={"code": "unauthorized", "message": "invalid or missing token"},
        )
    return hashlib.sha256(x_dlq_token.encode("utf-8")).hexdigest()[:12]


def _handle(fn: Callable):
    try:
        return fn()
    except DlqError as e:
        raise HTTPException(
            status_code=_HTTP_BY_CODE.get(e.code, 502),
            detail={"code": e.code, "message": str(e)},
        ) from e


def _audit(action: str, fingerprint: str, **fields) -> None:
    logger.info(
        "dlq admin action",
        extra={
            "action": action,
            "trace_id": ctx_trace_id.get(None),
            "token_fingerprint": fingerprint,
            **fields,
        },
    )


@router.get("/admin/dlq")
def get_stats(fp: str = Depends(dlq_auth)) -> DlqStats:
    stats = _handle(lambda: _get_service().stats())
    metrics.counter("dlq.depth", stats.available)
    metrics.counter("dlq.in_flight", stats.in_flight)
    return stats


@router.get("/admin/dlq/messages")
def peek(
    limit: int = Query(default=10, ge=1, le=10),
    hold_seconds: int | None = Query(default=None, ge=10, le=300),
    fp: str = Depends(dlq_auth),
) -> DlqPeekResult:
    hs = (
        hold_seconds if hold_seconds is not None else DlqAdminConfig().peek_hold_seconds
    )
    result = _handle(lambda: _get_service().peek(limit, hs))
    _audit("peek", fp, count=len(result.messages))
    return result


@router.post("/admin/dlq/redrive")
def redrive_all(body: RedriveAllRequest, fp: str = Depends(dlq_auth)) -> RedriveTask:
    if body.confirm is not True:
        raise HTTPException(
            status_code=400,
            detail={"code": "confirm_required", "message": "confirm must be true"},
        )
    task = _handle(
        lambda: _get_service().redrive_all(body.confirm, body.max_per_second)
    )
    metrics.counter("dlq.redrive_started", 1)
    _audit(
        "redrive_all",
        fp,
        task_handle=task.task_handle,
        max_per_second=body.max_per_second,
    )
    return task


@router.get("/admin/dlq/redrive")
def list_tasks(fp: str = Depends(dlq_auth)) -> list[RedriveTask]:
    return _handle(lambda: _get_service().list_tasks())


@router.delete("/admin/dlq/redrive/{task_handle}")
def cancel_task(task_handle: str, fp: str = Depends(dlq_auth)) -> RedriveTask:
    task = _handle(lambda: _get_service().cancel_task(task_handle))
    metrics.counter("dlq.redrive_cancel", 1)
    _audit("cancel_task", fp, task_handle=task_handle)
    return task


@router.post("/admin/dlq/messages/redrive")
def redrive_message(body: RedriveMessageRequest, fp: str = Depends(dlq_auth)) -> dict:
    body_sha256 = _handle(
        lambda: _get_service().redrive_message(body.receipt_handle, body.body)
    )
    metrics.counter("dlq.redrive_message", 1)
    _audit("redrive_message", fp, body_sha256=body_sha256)
    return {"status": "redriven", "body_sha256": body_sha256}
