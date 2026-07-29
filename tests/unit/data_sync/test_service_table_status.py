"""Post-sync table status log: one line showing whether each reference table
has rows, emitted after a successful reload."""

import logging
from unittest.mock import MagicMock
from uuid import uuid4

from app.data_sync import service
from app.data_sync.manifest import Manifest
from app.data_sync.service import REFERENCE_TABLES, _log_table_status

N_TABLES = len(REFERENCE_TABLES)


def _no_versions(monkeypatch) -> None:
    """Most status tests only care about counts; silence the version lookup."""
    monkeypatch.setattr(service, "_active_data_versions", lambda _session: {})


def test_logs_single_info_line_when_all_tables_have_rows(caplog, monkeypatch):
    _no_versions(monkeypatch)
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


def test_status_message_uses_context_label(caplog, monkeypatch):
    _no_versions(monkeypatch)
    session = MagicMock()
    session.scalar.return_value = 5

    with caplog.at_level(logging.INFO, logger="app.data_sync.service"):
        _log_table_status(session, context="Startup")

    records = [r for r in caplog.records if "Startup table status" in r.message]
    assert len(records) == 1
    assert "Post-sync" not in records[0].message


def test_warns_and_names_empty_tables(caplog, monkeypatch):
    _no_versions(monkeypatch)
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


def test_warns_and_names_tables_that_fail_to_count(caplog, monkeypatch):
    _no_versions(monkeypatch)
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


def test_never_raises_even_if_counting_blows_up(caplog, monkeypatch):
    _no_versions(monkeypatch)
    session = MagicMock()
    session.scalar.side_effect = RuntimeError("count failed")
    session.rollback.side_effect = RuntimeError("rollback failed too")

    with caplog.at_level(logging.WARNING, logger="app.data_sync.service"):
        _log_table_status(session)  # must not raise

    assert any("table status" in r.message for r in caplog.records)


def test_annotates_each_table_with_its_active_data_version(caplog, monkeypatch):
    monkeypatch.setattr(
        service,
        "_active_data_versions",
        lambda _session: {"coefficient_layer": "2026-06-01"},
    )
    session = MagicMock()
    session.scalar.return_value = 5

    with caplog.at_level(logging.INFO, logger="app.data_sync.service"):
        _log_table_status(session)

    message = next(r for r in caplog.records if "table status" in r.message).message
    assert "coefficient_layer=5@2026-06-01" in message
    # A table with no applied version is reported as a bare count.
    assert "edp_edges=5 " in message or message.endswith("edp_edges=5")


def test_status_still_logs_when_version_lookup_fails(caplog, monkeypatch):
    def boom(_session):
        msg = "no provenance"
        raise RuntimeError(msg)

    monkeypatch.setattr(service, "resolve_active_provenance", boom)
    session = MagicMock()
    session.scalar.return_value = 5

    with caplog.at_level(logging.INFO, logger="app.data_sync.service"):
        _log_table_status(session)

    records = [r for r in caplog.records if "Post-sync table status" in r.message]
    assert len(records) == 1
    assert "coefficient_layer=5" in records[0].message
    assert "all tables have rows" in records[0].message


def _do_run_with(monkeypatch, *, loaded: list[str]) -> tuple[MagicMock, MagicMock]:
    """Drive _do_run with everything stubbed; `loaded` is what _restore_all
    returns (the tables actually restored — empty means a no-op). Returns the
    _log_table_status and clear_spatial_caches mocks _do_run itself drives.
    """
    fake_session = MagicMock()
    fake_session.get.return_value = MagicMock()
    monkeypatch.setattr(service, "Session", lambda bind: fake_session)  # noqa: ARG005
    monkeypatch.setattr(service, "_build_s3_client", MagicMock())
    # A manifest with one table so the run's data_version summary can be built.
    manifest = Manifest(tables={"nn_catchments": {"key": "k", "version": "v1"}})
    monkeypatch.setattr(service, "_restore_all", MagicMock(return_value=loaded))
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
        manifest,
        force=False,
    )
    return log_status, clear_caches


def test_do_run_logs_table_status_after_successful_restore(monkeypatch):
    log_status, _ = _do_run_with(monkeypatch, loaded=["nn_catchments"])
    log_status.assert_called_once()


def test_do_run_clears_spatial_caches_after_successful_restore(monkeypatch):
    _, clear_caches = _do_run_with(monkeypatch, loaded=["nn_catchments"])
    clear_caches.assert_called_once()


def test_do_run_skips_log_and_clear_on_noop(monkeypatch):
    # When _restore_all loads nothing (all tables already at their version) it
    # reports table status itself; _do_run adds no extra log and leaves caches
    # intact since no reference data changed.
    log_status, clear_caches = _do_run_with(monkeypatch, loaded=[])
    log_status.assert_not_called()
    clear_caches.assert_not_called()
