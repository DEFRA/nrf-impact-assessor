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
from botocore.exceptions import ClientError

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


# -- DLQ fast-fail --


@pytest.fixture
def sqs_client_with_dlq():
    with patch("app.aws.sqs.boto3.client") as mock_client:
        mock_sqs = MagicMock()
        mock_client.return_value = mock_sqs
        client = SQSClient(
            queue_url="http://localhost:4566/000000000000/nrf-queue",
            region="eu-west-2",
            wait_time_seconds=1,
            visibility_timeout=30,
            max_messages=1,
            dlq_url="http://localhost:4566/000000000000/nrf-dlq",
        )
        client.sqs = mock_sqs
        yield client


def _raw(body: str, message_id="mid-1"):
    return {"ReceiptHandle": "rh-1", "Body": body, "MessageId": message_id}


def test_json_decode_failure_fast_fails_to_dlq(sqs_client_with_dlq):
    c = sqs_client_with_dlq
    c.sqs.receive_message.return_value = {"Messages": [_raw("{not json")]}
    results = c.receive_messages()
    assert results == []
    c.sqs.send_message.assert_called_once()
    kwargs = c.sqs.send_message.call_args.kwargs
    assert kwargs["QueueUrl"] == c.dlq_url
    attrs = kwargs["MessageAttributes"]
    assert attrs["failureReason"]["StringValue"] == "json-decode-error"
    assert attrs["sourceQueueUrl"]["StringValue"] == c.queue_url
    assert "rawBodyPreview" in attrs
    c.sqs.delete_message.assert_called_once_with(
        QueueUrl=c.queue_url, ReceiptHandle="rh-1"
    )


def test_sns_inner_decode_failure_fast_fails(sqs_client_with_dlq):
    c = sqs_client_with_dlq
    envelope = {"Type": "Notification", "Message": "{broken"}
    c.sqs.receive_message.return_value = {"Messages": [_raw(json.dumps(envelope))]}
    results = c.receive_messages()
    assert results == []
    attrs = c.sqs.send_message.call_args.kwargs["MessageAttributes"]
    assert attrs["failureReason"]["StringValue"] == "sns-message-decode-error"


def test_validation_error_fast_fails(sqs_client_with_dlq):
    c = sqs_client_with_dlq
    # Missing the required boundaryGeojson => ValidationError => fast-fail.
    c.sqs.receive_message.return_value = {
        "Messages": [_raw(json.dumps({"not": "a job"}))]
    }
    results = c.receive_messages()
    assert results == []
    attrs = c.sqs.send_message.call_args.kwargs["MessageAttributes"]
    assert attrs["failureReason"]["StringValue"] == "validation-error"


def test_oversized_fast_fails(sqs_client_with_dlq):
    c = sqs_client_with_dlq
    big = "x" * (262_144 + 1)
    c.sqs.receive_message.return_value = {"Messages": [_raw(big)]}
    results = c.receive_messages()
    assert results == []
    attrs = c.sqs.send_message.call_args.kwargs["MessageAttributes"]
    assert attrs["failureReason"]["StringValue"] == "oversized"


def test_poison_left_on_queue_when_no_dlq_configured(sqs_client):
    # sqs_client fixture has no dlq_url
    sqs_client.sqs.receive_message.return_value = {"Messages": [_raw("{not json")]}
    results = sqs_client.receive_messages()
    assert results == []
    sqs_client.sqs.send_message.assert_not_called()
    sqs_client.sqs.delete_message.assert_not_called()


def test_send_ok_delete_fail_does_not_raise(sqs_client_with_dlq):
    c = sqs_client_with_dlq
    c.sqs.receive_message.return_value = {"Messages": [_raw("{not json")]}
    c.sqs.delete_message.side_effect = ClientError(
        {"Error": {"Code": "X", "Message": "boom"}}, "DeleteMessage"
    )
    # Must not raise — message stays on source and will be reprocessed.
    assert c.receive_messages() == []
