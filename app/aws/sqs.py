"""SQS polling and message handling."""

import json
import logging
import re

import boto3
from botocore.exceptions import ClientError
from pydantic import ValidationError

from app.models.job import ImpactAssessmentJob

logger = logging.getLogger(__name__)

_max_body_bytes = 262_144  # SQS hard limit is 256 KiB

# EPSG codes the assessment pipeline can handle. Mirrors
# app.boundary.validation.SUPPORTED_CRS (kept local so the poll loop does not
# pull in geopandas). A geometry that declares any other CRS is rejected here
# rather than surviving into the outbound PATCH, which the backend rejects.
_supported_crs_epsg = {27700, 4326}
_epsg_pattern = re.compile(r"EPSG:{1,2}(\d+)$", re.IGNORECASE)


def _unsupported_declared_crs(job: ImpactAssessmentJob) -> str | None:
    """Return the offending CRS name if the boundary geometry declares an
    unsupported CRS, else None.

    GeoJSON geometries may carry a (deprecated) `crs` member of the form
    ``{"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::27700"}}``.
    A missing `crs` is valid (the normal case). A declared CRS must resolve to a
    supported EPSG code.
    """
    if job.boundary_geojson is None:
        return None
    geom = job.boundary_geojson.boundary_geometry_original
    crs = geom.get("crs") if isinstance(geom, dict) else None
    if not crs:
        return None
    props = crs.get("properties") if isinstance(crs, dict) else None
    name = props.get("name") if isinstance(props, dict) else None
    if not isinstance(name, str):
        return repr(name)
    match = _epsg_pattern.search(name)
    if match and int(match.group(1)) in _supported_crs_epsg:
        return None
    return name


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
        except ClientError:
            logger.exception("SQS receive_message failed")
            raise

        messages = response.get("Messages", [])
        results = []
        for raw_message in messages:
            job_message = self._parse_message(raw_message)
            if job_message is not None:
                results.append((job_message, raw_message["ReceiptHandle"]))

        return results

    def _parse_message(self, raw_message: dict) -> ImpactAssessmentJob | None:
        """Parse a single raw SQS message into a job, or None if invalid.

        Invalid messages (oversized, non-JSON, wrong schema, unsupported CRS)
        are logged and skipped. They are never deleted - the visibility timeout
        expires, the message retries and eventually moves to the DLQ after
        maxReceiveCount.
        """
        message_id = raw_message.get("MessageId")
        raw_body = raw_message["Body"]
        if len(raw_body) > _max_body_bytes:
            logger.error(
                "SQS message body exceeds size limit (%d bytes), skipping",
                len(raw_body),
                extra={"message_id": message_id},
            )
            return None
        try:
            body = json.loads(raw_body)
        except json.JSONDecodeError:
            logger.exception(
                "Message body is not valid JSON", extra={"message_id": message_id}
            )
            return None

        # Unwrap SNS envelope if present
        if body.get("Type") == "Notification" and "Message" in body:
            logger.debug("Unwrapping SNS envelope from SQS message")
            try:
                body = json.loads(body["Message"])
            except json.JSONDecodeError:
                logger.exception(
                    "SNS inner Message is not valid JSON",
                    extra={"message_id": message_id},
                )
                return None

        try:
            job_message = ImpactAssessmentJob.model_validate(body)
        except ValidationError:
            logger.exception(
                "Invalid job message format", extra={"message_id": message_id}
            )
            return None

        unsupported_crs = _unsupported_declared_crs(job_message)
        if unsupported_crs is not None:
            logger.error(
                "Boundary geometry declares unsupported CRS %r, skipping",
                unsupported_crs,
                extra={"message_id": message_id},
            )
            return None

        logger.info(f"Received job message: {job_message.reference}")
        return job_message

    def change_message_visibility(
        self, receipt_handle: str, visibility_timeout: int
    ) -> None:
        """Extend the visibility timeout of an in-flight message.

        Call periodically during long-running jobs to prevent SQS from
        re-delivering the message before processing completes.

        Never raises: the heartbeat daemon thread calls this, and a raise would
        kill the thread and silently stop visibility extensions (risking
        duplicate delivery mid-job). Failures are ERROR level so they are
        searchable in OpenSearch.
        """
        try:
            self.sqs.change_message_visibility(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle,
                VisibilityTimeout=visibility_timeout,
            )
        except Exception:
            logger.exception("Failed to extend message visibility timeout")

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
        except ClientError:
            logger.exception("Failed to delete message")
            raise
