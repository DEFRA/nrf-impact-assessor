"""SQS polling and message handling."""

import json
import logging
from datetime import UTC, datetime

import boto3
from botocore.exceptions import ClientError
from pydantic import ValidationError

from app.common import metrics
from app.models.job import ImpactAssessmentJob

logger = logging.getLogger(__name__)

_max_body_bytes = 262_144  # SQS hard limit is 256 KiB


class SQSClient:
    """Handles SQS message polling and lifecycle."""

    def __init__(
        self,
        queue_url: str,
        region: str,
        wait_time_seconds: int,
        visibility_timeout: int,
        max_messages: int,
        endpoint_url: str | None = None,
        dlq_url: str | None = None,
    ):
        self.queue_url = queue_url
        self.dlq_url = dlq_url
        self.region = region
        self.wait_time_seconds = wait_time_seconds
        self.visibility_timeout = visibility_timeout
        self.max_messages = max_messages
        client_kwargs: dict = {"region_name": region}
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url
        self.sqs = boto3.client("sqs", **client_kwargs)

    def receive_messages(self) -> list[tuple[ImpactAssessmentJob, str]]:
        """Poll SQS for job messages.

        Uses long polling to reduce empty receives. Returns empty list if queue is empty.
        Invalid messages are logged but not deleted - they retry until maxReceiveCount
        then move to DLQ.

        Returns:
            List of (ImpactAssessmentJob, receipt_handle) tuples. Empty if no valid messages.
        """
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=self.max_messages,
                WaitTimeSeconds=self.wait_time_seconds,
                VisibilityTimeout=self.visibility_timeout,
                MessageAttributeNames=["All"],
            )
        except ClientError as e:
            logger.error(f"SQS receive_message failed: {e}")
            raise

        messages = response.get("Messages", [])
        if not messages:
            return []

        results = []
        for raw_message in messages:
            receipt_handle = raw_message["ReceiptHandle"]
            raw_body = raw_message["Body"]

            if len(raw_body) > _max_body_bytes:
                self._fast_fail(
                    raw_message,
                    "oversized",
                    f"body {len(raw_body)} bytes exceeds limit {_max_body_bytes}",
                )
                continue

            try:
                body = json.loads(raw_body)
            except json.JSONDecodeError as e:
                self._fast_fail(raw_message, "json-decode-error", str(e))
                continue

            # Unwrap SNS envelope if present
            if body.get("Type") == "Notification" and "Message" in body:
                logger.debug("Unwrapping SNS envelope from SQS message")
                try:
                    body = json.loads(body["Message"])
                except json.JSONDecodeError as e:
                    self._fast_fail(raw_message, "sns-message-decode-error", str(e))
                    continue

            try:
                job_message = ImpactAssessmentJob.model_validate(body)
            except ValidationError as e:
                self._fast_fail(raw_message, "validation-error", str(e))
                continue

            logger.info(f"Received job message: {job_message.reference}")
            results.append((job_message, receipt_handle))

        return results

    def _fast_fail(self, raw_message: dict, reason: str, detail: str) -> None:
        """Remove a non-recoverable ('poison') message from the hot path.

        When a DLQ is configured, send the message to it with forensic context
        then delete it from the source. Without a DLQ, fall back to today's
        behaviour: log and leave on the queue (SQS will DLQ it after
        maxReceiveCount).
        """
        message_id = raw_message.get("MessageId")
        metrics.counter(f"consumer.poison_fast_fail.{reason}", 1)

        if not self.dlq_url:
            logger.error(
                "Poison message (%s) left on queue (no DLQ configured): %s",
                reason,
                detail,
                extra={"message_id": message_id},
            )
            return

        try:
            self.send_to_dlq(raw_message, reason, detail)
        except ClientError as e:
            metrics.counter("dlq.send_failure", 1)
            logger.error(
                "Failed to send poison message to DLQ; leaving on source: %s",
                e,
                extra={"message_id": message_id},
            )
            return

        try:
            self.sqs.delete_message(
                QueueUrl=self.queue_url, ReceiptHandle=raw_message["ReceiptHandle"]
            )
        except ClientError as e:
            metrics.counter("dlq.delete_failure", 1)
            logger.warning(
                "Poison message sent to DLQ but source delete failed; message "
                "may be reprocessed (id=%s): %s",
                message_id,
                e,
            )
            return

        logger.info(
            "Poison message fast-failed to DLQ (reason=%s, id=%s)",
            reason,
            message_id,
        )

    def send_to_dlq(self, raw_message: dict, reason: str, detail: str) -> None:
        """Send a poison message to the DLQ, preserving forensic context.

        Copies safe original message attributes and adds failure metadata.
        SQS allows at most 10 message attributes, so forensic attributes take
        precedence and any remaining slots are filled with copied originals.
        """
        body = raw_message["Body"]
        forensic: dict = {
            "originalMessageId": {
                "DataType": "String",
                "StringValue": raw_message.get("MessageId", "unknown"),
            },
            "sourceQueueUrl": {"DataType": "String", "StringValue": self.queue_url},
            "failureReason": {"DataType": "String", "StringValue": reason},
            "failureDetail": {"DataType": "String", "StringValue": detail[:1024]},
            "failedAt": {
                "DataType": "String",
                "StringValue": datetime.now(UTC).isoformat(),
            },
        }
        if reason in ("json-decode-error", "sns-message-decode-error"):
            forensic["rawBodyPreview"] = {
                "DataType": "String",
                "StringValue": body[:1024],
            }

        attrs = dict(forensic)
        budget = 10 - len(attrs)
        for key, value in (raw_message.get("MessageAttributes") or {}).items():
            if budget <= 0:
                break
            if key.startswith(("AWS.", "Amazon.")) or key in attrs:
                continue
            string_value = value.get("StringValue")
            if string_value is None:
                continue  # skip binary attributes
            attrs[key] = {
                "DataType": value.get("DataType", "String"),
                "StringValue": string_value,
            }
            budget -= 1

        self.sqs.send_message(
            QueueUrl=self.dlq_url, MessageBody=body, MessageAttributes=attrs
        )

    def change_message_visibility(
        self, receipt_handle: str, visibility_timeout: int
    ) -> None:
        """Extend the visibility timeout of an in-flight message.

        Call periodically during long-running jobs to prevent SQS from
        re-delivering the message before processing completes.
        """
        try:
            self.sqs.change_message_visibility(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle,
                VisibilityTimeout=visibility_timeout,
            )
        except ClientError as e:
            logger.warning(f"Failed to extend message visibility timeout: {e}")

    def delete_message(self, receipt_handle: str) -> None:
        """Delete message from queue after successful processing.

        Args:
            receipt_handle: Receipt handle from receive_message
        """
        try:
            self.sqs.delete_message(
                QueueUrl=self.queue_url, ReceiptHandle=receipt_handle
            )
            logger.info("Message deleted from queue")
        except ClientError as e:
            logger.error(f"Failed to delete message: {e}")
            raise
