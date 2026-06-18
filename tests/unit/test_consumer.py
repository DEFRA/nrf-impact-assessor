"""The consumer must only delete a message once its assessment has completed.
A failed job is left on the queue so SQS redelivers it (and eventually moves it
to the DLQ) rather than silently dropping the work."""

from unittest.mock import MagicMock

from app.consumer import SqsConsumer


def _drive_once(consumer: SqsConsumer, batch: list) -> None:
    """Run the poll loop for exactly one non-empty poll, then stop it."""
    state = {"polled": False}

    def _receive():
        if not state["polled"]:
            state["polled"] = True
            return batch
        consumer.running = False
        return []

    consumer.sqs_client.receive_messages.side_effect = _receive
    consumer.run()


def _consumer() -> SqsConsumer:
    return SqsConsumer(MagicMock(), MagicMock())


def test_deletes_message_when_processing_succeeds():
    consumer = _consumer()
    job = MagicMock(reference="NRF-000001")

    _drive_once(consumer, [(job, "receipt-1")])

    consumer.sqs_client.delete_message.assert_called_once_with("receipt-1")


def test_keeps_message_when_processing_raises():
    consumer = _consumer()
    consumer.orchestrator.process_job.side_effect = RuntimeError("assessment failed")
    job = MagicMock(reference="NRF-000001")

    _drive_once(consumer, [(job, "receipt-1")])

    consumer.sqs_client.delete_message.assert_not_called()


def test_one_failure_does_not_block_the_next_message():
    consumer = _consumer()
    consumer.orchestrator.process_job.side_effect = [RuntimeError("boom"), None]
    bad, good = MagicMock(reference="NRF-BAD"), MagicMock(reference="NRF-GOOD")

    _drive_once(consumer, [(bad, "receipt-bad"), (good, "receipt-good")])

    consumer.sqs_client.delete_message.assert_called_once_with("receipt-good")
