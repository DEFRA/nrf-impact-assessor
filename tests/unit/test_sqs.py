"""Unit tests for SQSClient message handling.

The backend publishes the CDP trace id inside the SNS message *body* as
`traceId` (see nrf-backend publish-quote-message.js), not as an SNS
MessageAttribute. These tests pin that contract: the trace id must survive
SNS-envelope unwrapping and land on `job.trace_id`, which the orchestrator
then propagates onto the outbound PATCH callback.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from app.aws.sqs import SQSClient

# -- Fixtures --

_INNER_JOB = {
    "reference": "NRF-000001",
    "boundaryGeojson": {
        "boundaryGeometryOriginal": {
            "type": "Polygon",
            "coordinates": [
                [
                    [582814.93, 328188.89],
                    [582808.89, 328203.73],
                    [582824.96, 328210.17],
                    [582830.09, 328197.00],
                    [582814.93, 328188.89],
                ]
            ],
        },
        "intersectingEdps": [
            {"label": "River Wensum SAC", "n2k_site_name": "River Wensum SAC"}
        ],
    },
    "developmentTypes": ["housing"],
    "residentialBuildingCount": 25,
}


def _sns_envelope(inner: dict) -> dict:
    return {
        "Type": "Notification",
        "MessageId": "sns-message-id",
        "TopicArn": "arn:aws:sns:eu-west-2:000000000000:nrf-topic",
        "Message": json.dumps(inner),
    }


def _sqs_message(body: dict) -> dict:
    return {"ReceiptHandle": "receipt-handle-1", "Body": json.dumps(body)}


@pytest.fixture
def sqs_client():
    """SQSClient with a mocked boto3 client exposed as `.sqs`."""
    with patch("app.aws.sqs.boto3.client") as mock_client:
        mock_sqs = MagicMock()
        mock_client.return_value = mock_sqs
        client = SQSClient(
            queue_url="http://localhost:4566/000000000000/nrf-queue",
            region="eu-west-2",
            wait_time_seconds=1,
            visibility_timeout=30,
            max_messages=1,
        )
        client.sqs = mock_sqs
        yield client


def _receive_one(client: SQSClient, body: dict):
    client.sqs.receive_message.return_value = {"Messages": [_sqs_message(body)]}
    results = client.receive_messages()
    assert len(results) == 1
    job, receipt_handle = results[0]
    assert receipt_handle == "receipt-handle-1"
    return job


class TestChangeMessageVisibility:
    """The visibility heartbeat thread calls this method; it must never raise
    (a raise would kill the daemon thread and stop visibility extensions) and
    failures must be ERROR level so they are searchable in OpenSearch."""

    def test_client_error_is_logged_as_error_and_not_raised(self, sqs_client, caplog):
        from botocore.exceptions import ClientError

        sqs_client.sqs.change_message_visibility.side_effect = ClientError(
            {"Error": {"Code": "InternalError", "Message": "boom"}},
            "ChangeMessageVisibility",
        )
        with caplog.at_level("ERROR", logger="app.aws.sqs"):
            sqs_client.change_message_visibility("rh-1", 300)
        assert any(r.levelname == "ERROR" for r in caplog.records)

    def test_unexpected_error_is_logged_as_error_and_not_raised(
        self, sqs_client, caplog
    ):
        from botocore.exceptions import EndpointConnectionError

        sqs_client.sqs.change_message_visibility.side_effect = EndpointConnectionError(
            endpoint_url="http://localhost:4566"
        )
        with caplog.at_level("ERROR", logger="app.aws.sqs"):
            sqs_client.change_message_visibility("rh-1", 300)
        assert any(r.levelname == "ERROR" for r in caplog.records)


class TestInvalidJson:
    def test_non_json_body_is_skipped_without_raising(self, sqs_client):
        """A body that isn't JSON is logged and left on the queue, not raised."""
        sqs_client.sqs.receive_message.return_value = {
            "Messages": [
                {"ReceiptHandle": "rh-1", "Body": "{not json", "MessageId": "m-1"}
            ]
        }
        assert sqs_client.receive_messages() == []
        sqs_client.sqs.delete_message.assert_not_called()

    def test_broken_sns_inner_message_is_skipped_without_raising(self, sqs_client):
        """An SNS envelope whose inner Message isn't JSON is logged and skipped."""
        envelope = {"Type": "Notification", "Message": "{broken"}
        sqs_client.sqs.receive_message.return_value = {
            "Messages": [
                {
                    "ReceiptHandle": "rh-1",
                    "Body": json.dumps(envelope),
                    "MessageId": "m-1",
                }
            ]
        }
        assert sqs_client.receive_messages() == []
        sqs_client.sqs.delete_message.assert_not_called()


class TestTracePropagation:
    def test_trace_id_extracted_from_sns_message_body(self, sqs_client):
        """SNS-wrapped message: body traceId survives unwrap -> job.trace_id."""
        envelope = _sns_envelope({**_INNER_JOB, "traceId": "trace-from-sns"})
        job = _receive_one(sqs_client, envelope)
        assert job.trace_id == "trace-from-sns"

    def test_trace_id_extracted_from_raw_message_body(self, sqs_client):
        """Raw delivery (no SNS envelope): top-level traceId -> job.trace_id."""
        job = _receive_one(sqs_client, {**_INNER_JOB, "traceId": "trace-raw"})
        assert job.trace_id == "trace-raw"

    def test_missing_trace_id_leaves_trace_id_unset(self, sqs_client):
        """No traceId in the message body -> job.trace_id stays None."""
        envelope = _sns_envelope(_INNER_JOB)
        job = _receive_one(sqs_client, envelope)
        assert job.reference == "NRF-000001"
        assert job.trace_id is None
