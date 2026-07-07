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


# -- Readiness gate + backoff --


class _FakeGate:
    def __init__(self, sequence):
        self._seq = list(sequence)
        self.calls = 0

    def ok(self):
        val = self._seq[min(self.calls, len(self._seq) - 1)]
        self.calls += 1
        return val


class _RecordingBackoff:
    def __init__(self):
        self.delays = []
        self.resets = 0

    def next_delay(self):
        self.delays.append(0)  # 0 so tests don't actually sleep
        return 0

    def reset(self):
        self.resets += 1


def _gated_consumer(sqs_client, gate, backoff) -> SqsConsumer:
    return SqsConsumer(
        sqs_client=sqs_client,
        orchestrator=MagicMock(),
        readiness_gate=gate,
        backoff=backoff,
    )


def test_gate_closed_skips_poll_and_backs_off(monkeypatch):
    sqs_client = MagicMock()
    gate = _FakeGate([False])
    backoff = _RecordingBackoff()
    c = _gated_consumer(sqs_client, gate, backoff)

    def stop_after(*_a, **_k):
        c.running = False  # break out of the loop after the first backoff sleep

    # monkeypatch auto-reverts, so we don't leak a patched time.sleep to other tests
    monkeypatch.setattr("app.consumer.time.sleep", stop_after)
    c.run()

    sqs_client.receive_messages.assert_not_called()
    assert backoff.delays  # backed off at least once


def test_successful_poll_resets_backoff():
    sqs_client = MagicMock()
    gate = _FakeGate([True])
    backoff = _RecordingBackoff()
    c = _gated_consumer(sqs_client, gate, backoff)

    def maybe_stop():
        c.running = False  # stop after the first successful (empty) poll
        return []

    sqs_client.receive_messages.side_effect = maybe_stop
    c.run()

    assert backoff.resets >= 1
