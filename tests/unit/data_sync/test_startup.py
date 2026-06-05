"""Startup-time validation for the data-sync configuration."""

import importlib
import logging


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
