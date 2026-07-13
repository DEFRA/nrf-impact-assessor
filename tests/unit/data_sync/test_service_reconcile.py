"""Unit tests for DataLoadHistory reconciliation (DM-5)."""

from unittest.mock import MagicMock
from uuid import uuid4

from app.data_sync import service
from app.data_sync.service import _reconcile_load_history


def _manifest(version="2026.07.01", tables=None):
    m = MagicMock()
    m.data_version = version
    m.tables = tables or {"nn_catchments": "k1", "coefficient_layer": "k2"}
    return m


def test_reconcile_backfills_table_missing_from_history():
    session = MagicMock()
    # nn_catchments has a live MAX(version) but no history row; coefficient_layer is fine.
    session.scalar.side_effect = [
        5,  # MAX(version) nn_catchments
        0,  # history count for nn_catchments at that version -> missing
        5,  # MAX(version) coefficient_layer
        1,  # history count for coefficient_layer -> present
    ]
    run_id = uuid4()

    backfilled = _reconcile_load_history(session, run_id, _manifest())

    assert backfilled == ["nn_catchments"]
    assert session.add.call_count == 1
    added = session.add.call_args.args[0]
    assert added.table_name == "nn_catchments"
    assert added.status == "reconciled"
    session.commit.assert_called_once()


def test_reconcile_noop_when_history_consistent():
    session = MagicMock()
    session.scalar.side_effect = [5, 1, 5, 1]  # both tables present
    backfilled = _reconcile_load_history(session, uuid4(), _manifest())
    assert backfilled == []
    session.add.assert_not_called()


def test_do_run_reconciles_before_reload_decision(monkeypatch):
    session = MagicMock()
    run = MagicMock()
    session.get.return_value = run
    monkeypatch.setattr(service, "Session", lambda bind: session)  # noqa: ARG005
    monkeypatch.setattr(service, "_build_s3_client", lambda cfg, aws: MagicMock())  # noqa: ARG005
    monkeypatch.setattr(service, "_last_applied_version", lambda s: "2026.07.01")  # noqa: ARG005
    monkeypatch.setattr(service, "_log_table_status", lambda *a, **k: None)  # noqa: ARG005
    monkeypatch.setattr(service, "_finish", lambda *a, **k: None)  # noqa: ARG005
    called = {}
    monkeypatch.setattr(
        service,
        "_reconcile_load_history",
        lambda s, rid, m: called.setdefault("ran", True) or [],  # noqa: ARG005
    )

    # manifest version equals last applied -> no-op reload, but reconcile still runs.
    service._do_run(
        MagicMock(),
        MagicMock(),
        MagicMock(),
        MagicMock(),
        "eu-west-2",
        uuid4(),
        _manifest(version="2026.07.01"),
        force=False,
    )

    assert called.get("ran") is True
