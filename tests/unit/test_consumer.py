"""The consumer must only delete a message once its assessment has completed.
A failed job is left on the queue so SQS redelivers it (and eventually moves it
to the DLQ) rather than silently dropping the work."""

from unittest.mock import MagicMock

from app import consumer
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


def test_run_api_server_gives_uvicorn_our_logging_config(monkeypatch):
    """Uvicorn must be handed the app's logging config, not left to install its
    own — its default re-points uvicorn.error at a plain-text stderr handler
    with propagate=false, so an unhandled ASGI exception reaches CDP as one
    document per traceback line, all tagged error and none carrying trace.id.
    """
    fake_uvicorn = MagicMock()
    monkeypatch.setattr(consumer, "uvicorn", fake_uvicorn)

    consumer.run_api_server("0.0.0.0", 8085)  # noqa: S104

    kwargs = fake_uvicorn.run.call_args.kwargs
    assert kwargs["log_config"] == consumer.load_logging_config()


def test_logging_config_lets_uvicorn_error_reach_the_ecs_handler(monkeypatch):
    """The config above only helps if it leaves uvicorn's loggers propagating to
    the root ECS handler; an override there would put the traceback back on a
    handler of its own.
    """
    monkeypatch.setenv("ECS_CONTAINER_METADATA_URI_V4", "http://169.254.170.2/v4/x")

    config = consumer.load_logging_config()

    assert config["formatters"]["ecs"]["()"] == "ecs_logging.StdlibFormatter"
    assert config["root"]["handlers"] == ["console"]
    for name, logger in config.get("loggers", {}).items():
        assert "handlers" not in logger, f"{name} bypasses the root ECS handler"
