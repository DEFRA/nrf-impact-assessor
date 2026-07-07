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


def test_status_message_uses_context_label(caplog):
    session = MagicMock()
    session.scalar.return_value = 5

    with caplog.at_level(logging.INFO, logger="app.data_sync.service"):
        _log_table_status(session, context="Startup")

    records = [r for r in caplog.records if "Startup table status" in r.message]
    assert len(records) == 1
    assert "Post-sync" not in records[0].message


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


def _do_run_with(monkeypatch, *, reload_needed: bool) -> tuple[MagicMock, MagicMock]:
    """Drive _do_run with everything stubbed; return relevant collaborator mocks."""
    fake_session = MagicMock()
    fake_session.get.return_value = MagicMock()
    monkeypatch.setattr(service, "Session", lambda bind: fake_session)  # noqa: ARG005
    monkeypatch.setattr(service, "_build_s3_client", MagicMock())
    monkeypatch.setattr(service, "_last_applied_version", MagicMock(return_value=None))
    monkeypatch.setattr(service, "needs_reload", MagicMock(return_value=reload_needed))
    monkeypatch.setattr(service, "_restore_all", MagicMock())
    log_status = MagicMock()
    monkeypatch.setattr(service, "_log_table_status", log_status)
    clear_caches = MagicMock()
    monkeypatch.setattr(service, "clear_spatial_caches", clear_caches)

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
    return log_status, clear_caches


def test_do_run_logs_table_status_after_successful_restore(monkeypatch):
    log_status, _ = _do_run_with(monkeypatch, reload_needed=True)
    log_status.assert_called_once()


def test_do_run_clears_spatial_caches_after_successful_restore(monkeypatch):
    _, clear_caches = _do_run_with(monkeypatch, reload_needed=True)
    clear_caches.assert_called_once()


def test_do_run_logs_table_status_on_noop(monkeypatch):
    # A no-op sync still reports table status so an empty reference table is
    # visible even when the data version is already applied and no reload runs.
    log_status, clear_caches = _do_run_with(monkeypatch, reload_needed=False)
    log_status.assert_called_once()
    # No reload happened, so caches are left intact.
    clear_caches.assert_not_called()


def test_reference_tables_populated_true_when_all_have_rows():
    from app.data_sync.service import (
        _CRITICAL_REFERENCE_TABLES,
        reference_tables_populated,
    )

    session = MagicMock()
    session.scalar.return_value = 5
    assert reference_tables_populated(session) is True
    assert session.scalar.call_count == len(_CRITICAL_REFERENCE_TABLES)


def test_reference_tables_populated_false_when_any_empty():
    from app.data_sync.service import reference_tables_populated

    session = MagicMock()
    session.scalar.side_effect = [5, 0]  # second critical table empty
    assert reference_tables_populated(session) is False


def test_reference_tables_populated_false_on_count_error():
    from app.data_sync.service import reference_tables_populated

    session = MagicMock()
    session.scalar.side_effect = RuntimeError("relation does not exist")
    assert reference_tables_populated(session) is False
    session.rollback.assert_called_once()
