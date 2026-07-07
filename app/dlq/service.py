"""Service wrapping boto3 SQS calls for DLQ inspection and redrive."""

import hashlib
import logging
from datetime import UTC, datetime, timedelta

import boto3
from botocore.exceptions import ClientError

from app.dlq.models import DlqMessage, DlqPeekResult, DlqStats, RedriveTask

logger = logging.getLogger(__name__)


class DlqError(Exception):
    """Base DLQ service error carrying a stable string code for the router."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


class HandleExpiredError(DlqError):
    def __init__(self, message: str = "receipt handle expired; re-peek"):
        super().__init__("handle_expired", message)


class DlqUnavailableError(DlqError):
    def __init__(self, message: str = "DLQ is not available"):
        super().__init__("dlq_unavailable", message)


class InvalidParameterError(DlqError):
    def __init__(self, message: str):
        super().__init__("invalid_parameter", message)


class SqsError(DlqError):
    def __init__(self, message: str):
        super().__init__("sqs_error", message)


def _is_stale_handle(err: ClientError) -> bool:
    error = err.response.get("Error", {})
    code = error.get("Code", "")
    message = error.get("Message", "")
    if code == "ReceiptHandleIsInvalid":
        return True
    # InvalidParameterValue is only a stale-handle signal when it names the
    # receipt handle; other causes (e.g. bad VisibilityTimeout) are real 400s.
    return code == "InvalidParameterValue" and "ReceiptHandle" in message


def _epoch_millis_to_dt(value) -> datetime | None:
    if value is None:
        return None
    return datetime.fromtimestamp(int(value) / 1000, tz=UTC)


class DlqService:
    def __init__(
        self,
        dlq_url: str,
        main_queue_url: str,
        region: str,
        endpoint_url: str | None = None,
        body_preview_limit: int = 4096,
    ):
        if not dlq_url:
            msg = "AWS_SQS_DLQ_URL is not configured"
            raise DlqUnavailableError(msg)
        self.dlq_url = dlq_url
        self.main_queue_url = main_queue_url
        self.body_preview_limit = body_preview_limit
        client_kwargs: dict = {"region_name": region}
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url
        self.sqs = boto3.client("sqs", **client_kwargs)
        self._dlq_arn: str | None = None

    def _map(self, err: ClientError) -> DlqError:
        if _is_stale_handle(err):
            return HandleExpiredError()
        code = err.response.get("Error", {}).get("Code", "")
        if code == "InvalidParameterValue":
            return InvalidParameterError(str(err))
        if code == "QueueDoesNotExist":
            return DlqUnavailableError(str(err))
        return SqsError(str(err))

    def _arn(self) -> str:
        if self._dlq_arn is None:
            try:
                resp = self.sqs.get_queue_attributes(
                    QueueUrl=self.dlq_url, AttributeNames=["QueueArn"]
                )
            except ClientError as e:
                raise self._map(e) from e
            self._dlq_arn = resp["Attributes"]["QueueArn"]
        return self._dlq_arn

    def stats(self) -> DlqStats:
        try:
            resp = self.sqs.get_queue_attributes(
                QueueUrl=self.dlq_url,
                AttributeNames=[
                    "ApproximateNumberOfMessages",
                    "ApproximateNumberOfMessagesNotVisible",
                ],
            )
        except ClientError as e:
            raise self._map(e) from e
        attrs = resp["Attributes"]
        return DlqStats(
            available=int(attrs.get("ApproximateNumberOfMessages", 0)),
            in_flight=int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0)),
        )

    def peek(self, limit: int, hold_seconds: int) -> DlqPeekResult:
        limit = max(1, min(int(limit), 10))
        hold_seconds = max(10, min(int(hold_seconds), 300))
        try:
            resp = self.sqs.receive_message(
                QueueUrl=self.dlq_url,
                MaxNumberOfMessages=limit,
                VisibilityTimeout=hold_seconds,
                WaitTimeSeconds=0,
                MessageAttributeNames=["All"],
                AttributeNames=["ApproximateReceiveCount", "SentTimestamp"],
            )
        except ClientError as e:
            raise self._map(e) from e

        deadline = datetime.now(UTC) + timedelta(seconds=hold_seconds)
        messages: list[DlqMessage] = []
        for m in resp.get("Messages", []):
            body = m["Body"]
            attrs = m.get("Attributes", {})
            messages.append(
                DlqMessage(
                    message_id=m["MessageId"],
                    receipt_handle=m["ReceiptHandle"],
                    body=body,
                    body_preview=body[: self.body_preview_limit],
                    body_truncated=len(body) > self.body_preview_limit,
                    body_bytes=len(body),
                    receive_count=int(attrs.get("ApproximateReceiveCount", 0)),
                    sent_at=_epoch_millis_to_dt(attrs.get("SentTimestamp")),
                )
            )
        return DlqPeekResult(
            messages=messages,
            visibility_deadline=deadline,
            hold_seconds=hold_seconds,
        )

    def redrive_all(
        self, confirm: bool, max_per_second: int | None = None
    ) -> RedriveTask:
        if confirm is not True:
            msg = "confirm must be true for bulk redrive"
            raise InvalidParameterError(msg)
        arn = self._arn()
        kwargs: dict = {"SourceArn": arn}
        if max_per_second is not None:
            kwargs["MaxNumberOfMessagesPerSecond"] = int(max_per_second)
        try:
            resp = self.sqs.start_message_move_task(**kwargs)
        except ClientError as e:
            raise self._map(e) from e
        return RedriveTask(
            task_handle=resp.get("TaskHandle"),
            status="RUNNING",
            source_arn=arn,
        )

    def list_tasks(self) -> list[RedriveTask]:
        try:
            resp = self.sqs.list_message_move_tasks(SourceArn=self._arn())
        except ClientError as e:
            raise self._map(e) from e
        tasks: list[RedriveTask] = []
        for r in resp.get("Results", []):
            tasks.append(
                RedriveTask(
                    task_handle=r.get("TaskHandle"),
                    status=r.get("Status"),
                    approx_messages_moved=r.get("ApproximateNumberOfMessagesMoved"),
                    source_arn=r.get("SourceArn"),
                    started_at=_epoch_millis_to_dt(r.get("StartedTimestamp")),
                )
            )
        return tasks

    def cancel_task(self, task_handle: str) -> RedriveTask:
        try:
            resp = self.sqs.cancel_message_move_task(TaskHandle=task_handle)
        except ClientError as e:
            raise self._map(e) from e
        return RedriveTask(
            task_handle=task_handle,
            status="CANCELLED",
            approx_messages_moved=resp.get("ApproximateNumberOfMessagesMoved"),
        )

    def redrive_message(self, receipt_handle: str, body: str) -> str:
        """Send body to the main queue, then delete from the DLQ.

        Returns the sha256 of the body sent (for the audit log). If send
        succeeds but delete fails, the message may exist in both queues — see the
        design's Idempotency section.
        """
        try:
            self.sqs.send_message(QueueUrl=self.main_queue_url, MessageBody=body)
        except ClientError as e:
            raise self._map(e) from e
        try:
            self.sqs.delete_message(QueueUrl=self.dlq_url, ReceiptHandle=receipt_handle)
        except ClientError as e:
            raise self._map(e) from e
        return hashlib.sha256(body.encode("utf-8")).hexdigest()
