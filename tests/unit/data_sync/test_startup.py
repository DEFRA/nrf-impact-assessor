"""Startup-time validation for the data-sync configuration."""

import importlib
import logging
from unittest.mock import MagicMock

from app.data_sync import service


def test_log_startup_table_status_warns_on_empty(monkeypatch, caplog):
    session = MagicMock()
    session.scalar.return_value = 0
    repo = MagicMock()
    repo.session.return_value.__enter__.return_value = session
    monkeypatch.setattr(service, "get_shared_repository", lambda: repo)

    with caplog.at_level(logging.INFO, logger="app.data_sync.service"):
        service.log_startup_table_status()

    records = [r for r in caplog.records if "Startup table status" in r.message]
    assert len(records) == 1
    assert records[0].levelno == logging.WARNING
    assert "EMPTY:" in records[0].message


def test_log_startup_table_status_never_raises(monkeypatch, caplog):
    monkeypatch.setattr(
        service,
        "get_shared_repository",
        MagicMock(side_effect=RuntimeError("no engine")),
    )

    with caplog.at_level(logging.WARNING, logger="app.data_sync.service"):
        service.log_startup_table_status()  # must not raise

    assert any("startup table status" in r.message for r in caplog.records)


def test_startup_warns_when_enabled_without_bucket(monkeypatch, caplog):
    """Mounting data-sync without a bucket warns but does not stop startup."""
    monkeypatch.setenv("DATA_SYNC_ENABLED", "true")
    monkeypatch.setenv("DATA_SYNC_S3_BUCKET", "")

    import app.main

    try:
        with caplog.at_level(logging.WARNING, logger="app.main"):
            importlib.reload(app.main)  # must not raise
        assert any(
            "DATA_SYNC_S3_BUCKET is not set" in r.message for r in caplog.records
        )
    finally:
        # Restore a clean module state (data-sync disabled) for other tests.
        monkeypatch.setenv("DATA_SYNC_ENABLED", "false")
        importlib.reload(app.main)


def test_startup_ok_when_enabled_with_bucket(monkeypatch):
    monkeypatch.setenv("DATA_SYNC_ENABLED", "true")
    monkeypatch.setenv("DATA_SYNC_S3_BUCKET", "ref-data")
    monkeypatch.setenv("DATA_SYNC_S3_PREFIX", "reference_data_backups")

    import app.main

    try:
        importlib.reload(app.main)  # must not raise
    finally:
        monkeypatch.setenv("DATA_SYNC_ENABLED", "false")
        importlib.reload(app.main)
