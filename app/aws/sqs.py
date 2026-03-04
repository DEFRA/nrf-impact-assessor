"""SQS polling and message handling."""

import json
import logging

import boto3
from botocore.exceptions import ClientError
from pydantic import ValidationError

from app.models.job import ImpactAssessmentJob

logger = logging.getLogger(__name__)


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
    ):
        self.queue_url = queue_url
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
            body = json.loads(raw_message["Body"])

            try:
                job_message = ImpactAssessmentJob.model_validate(body)
                logger.info(f"Received job message: {job_message.job_id}")
                results.append((job_message, receipt_handle))
            except ValidationError as e:
                logger.error(
                    f"Invalid job message format: {e}",
                    extra={
                        "message_id": raw_message.get("MessageId"),
                        "body": body,
                    },
                )
                # Don't delete - let visibility timeout expire
                # Message will retry and eventually move to DLQ after maxReceiveCount

        return results

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
