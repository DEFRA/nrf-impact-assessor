"""Post-sync table status log: one line showing whether each reference table
has rows, emitted after a successful reload."""

import logging
from unittest.mock import MagicMock
from uuid import uuid4

from app.data_sync import service
from app.data_sync.service import _log_table_status

N_TABLES = 10


def test_logs_single_info_line_when_all_tables_have_rows(caplog):
    session = MagicMock()
    session.scalar.return_value = 5

    with caplog.at_level(logging.INFO, logger="app.data_sync.service"):
        _log_table_status(session)

    records = [r for r in caplog.records if "Post-sync table status" in r.message]
    assert len(records) == 1
    record = records[0]
    assert record.levelno == logging.INFO
    assert "coefficient_layer=5" in record.message
    assert "edp_edges=5" in record.message
    assert "all tables have rows" in record.message


def test_warns_and_names_empty_tables(caplog):
    session = MagicMock()
    # lookup_table (3rd in the list) comes back empty
    counts = [5] * N_TABLES
    counts[2] = 0
    session.scalar.side_effect = counts

    with caplog.at_level(logging.INFO, logger="app.data_sync.service"):
        _log_table_status(session)

    records = [r for r in caplog.records if "Post-sync table status" in r.message]
    assert len(records) == 1
    record = records[0]
    assert record.levelno == logging.WARNING
    assert "EMPTY: lookup_table" in record.message
    assert "all tables have rows" not in record.message


def test_warns_and_names_tables_that_fail_to_count(caplog):
    session = MagicMock()
    effects: list = [5] * N_TABLES
    effects[0] = RuntimeError("boom")
    session.scalar.side_effect = effects

    with caplog.at_level(logging.INFO, logger="app.data_sync.service"):
        _log_table_status(session)

    records = [r for r in caplog.records if "Post-sync table status" in r.message]
    assert len(records) == 1
    record = records[0]
    assert record.levelno == logging.WARNING
    assert "ERROR: coefficient_layer" in record.message
    session.rollback.assert_called_once()


def test_never_raises_even_if_counting_blows_up(caplog):
    session = MagicMock()
    session.scalar.side_effect = RuntimeError("count failed")
    session.rollback.side_effect = RuntimeError("rollback failed too")

    with caplog.at_level(logging.WARNING, logger="app.data_sync.service"):
        _log_table_status(session)  # must not raise

    assert any("table status" in r.message for r in caplog.records)


def _do_run_with(monkeypatch, *, reload_needed: bool) -> MagicMock:
    """Drive _do_run with everything stubbed; return the _log_table_status mock."""
    fake_session = MagicMock()
    fake_session.get.return_value = MagicMock()
    monkeypatch.setattr(service, "Session", lambda bind: fake_session)  # noqa: ARG005
    monkeypatch.setattr(service, "_build_s3_client", MagicMock())
    monkeypatch.setattr(service, "_last_applied_version", MagicMock(return_value=None))
    monkeypatch.setattr(service, "needs_reload", MagicMock(return_value=reload_needed))
    monkeypatch.setattr(service, "_restore_all", MagicMock())
    log_status = MagicMock()
    monkeypatch.setattr(service, "_log_table_status", log_status)

    service._do_run(
        MagicMock(),  # engine
        MagicMock(),  # cfg
        MagicMock(),  # aws
        MagicMock(),  # db
        "eu-west-2",
        uuid4(),
        MagicMock(),  # manifest
        force=False,
    )
    return log_status


def test_do_run_logs_table_status_after_successful_restore(monkeypatch):
    log_status = _do_run_with(monkeypatch, reload_needed=True)
    log_status.assert_called_once()


def test_do_run_skips_table_status_on_noop(monkeypatch):
    log_status = _do_run_with(monkeypatch, reload_needed=False)
    log_status.assert_not_called()
