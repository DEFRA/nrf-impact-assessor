import json
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from app.dlq.service import (
    DlqService,
    HandleExpiredError,
    InvalidParameterError,
    SqsError,
)


@pytest.fixture
def service():
    with patch("app.dlq.service.boto3.client") as mock_client:
        mock_sqs = MagicMock()
        mock_client.return_value = mock_sqs
        svc = DlqService(
            dlq_url="http://localhost:4566/000000000000/nrf-dlq",
            main_queue_url="http://localhost:4566/000000000000/nrf-queue",
            region="eu-west-2",
            body_preview_limit=8,
        )
        svc.sqs = mock_sqs
        yield svc


def test_stats(service):
    service.sqs.get_queue_attributes.return_value = {
        "Attributes": {
            "ApproximateNumberOfMessages": "3",
            "ApproximateNumberOfMessagesNotVisible": "1",
        }
    }
    stats = service.stats()
    assert stats.available == 3
    assert stats.in_flight == 1


def test_peek_returns_full_body_and_truncated_preview(service):
    service.sqs.receive_message.return_value = {
        "Messages": [
            {
                "MessageId": "m1",
                "ReceiptHandle": "rh1",
                "Body": "0123456789ABCDEF",  # 16 chars, limit is 8
                "Attributes": {
                    "ApproximateReceiveCount": "4",
                    "SentTimestamp": "1720000000000",
                },
            }
        ]
    }
    result = service.peek(limit=5, hold_seconds=30)
    msg = result.messages[0]
    assert msg.body == "0123456789ABCDEF"  # full
    assert msg.body_preview == "01234567"  # 8 bytes
    assert msg.body_truncated is True
    assert msg.body_bytes == 16
    assert msg.receive_count == 4
    assert result.hold_seconds == 30
    # visibility_deadline ~ now + 30s
    assert result.visibility_deadline is not None


def test_peek_clamps_limit_and_hold(service):
    service.sqs.receive_message.return_value = {"Messages": []}
    service.peek(limit=999, hold_seconds=9999)
    kwargs = service.sqs.receive_message.call_args.kwargs
    assert kwargs["MaxNumberOfMessages"] == 10
    assert kwargs["VisibilityTimeout"] == 300


def test_redrive_all_requires_confirm(service):
    with pytest.raises(InvalidParameterError):
        service.redrive_all(confirm=False)


def test_redrive_all_passes_velocity(service):
    service.sqs.get_queue_attributes.return_value = {
        "Attributes": {"QueueArn": "arn:aws:sqs:eu-west-2:0:nrf-dlq"}
    }
    service.sqs.start_message_move_task.return_value = {"TaskHandle": "th1"}
    task = service.redrive_all(confirm=True, max_per_second=5)
    kwargs = service.sqs.start_message_move_task.call_args.kwargs
    assert kwargs["SourceArn"] == "arn:aws:sqs:eu-west-2:0:nrf-dlq"
    assert kwargs["MaxNumberOfMessagesPerSecond"] == 5
    assert task.task_handle == "th1"


def test_redrive_message_sends_then_deletes_and_returns_hash(service):
    import hashlib

    body = json.dumps({"reference": "NRF-1"})
    sha = service.redrive_message(receipt_handle="rh1", body=body)
    service.sqs.send_message.assert_called_once_with(
        QueueUrl=service.main_queue_url, MessageBody=body
    )
    service.sqs.delete_message.assert_called_once_with(
        QueueUrl=service.dlq_url, ReceiptHandle="rh1"
    )
    assert sha == hashlib.sha256(body.encode()).hexdigest()


def test_stale_handle_maps_to_handle_expired(service):
    service.sqs.delete_message.side_effect = ClientError(
        {"Error": {"Code": "ReceiptHandleIsInvalid", "Message": "bad"}},
        "DeleteMessage",
    )
    with pytest.raises(HandleExpiredError):
        service.redrive_message(receipt_handle="stale", body="{}")


def test_invalid_parameter_not_about_handle_maps_to_invalid_parameter(service):
    service.sqs.receive_message.side_effect = ClientError(
        {
            "Error": {
                "Code": "InvalidParameterValue",
                "Message": "Value for VisibilityTimeout is invalid",
            }
        },
        "ReceiveMessage",
    )
    with pytest.raises(InvalidParameterError):
        service.peek(limit=1, hold_seconds=30)


def test_generic_client_error_maps_to_sqs_error(service):
    service.sqs.get_queue_attributes.side_effect = ClientError(
        {"Error": {"Code": "InternalError", "Message": "boom"}},
        "GetQueueAttributes",
    )
    with pytest.raises(SqsError):
        service.stats()
