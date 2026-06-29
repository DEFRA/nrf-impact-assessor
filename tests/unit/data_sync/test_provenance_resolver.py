"""Unit tests for resolve_active_provenance (DM-3)."""

from unittest.mock import MagicMock
from uuid import uuid4

from app.data_sync.service import resolve_active_provenance


def test_resolves_latest_successful_run():
    rid = uuid4()
    session = MagicMock()
    chain = session.query.return_value.filter.return_value.order_by.return_value
    chain.first.return_value = (rid, "2026.07.01")
    p = resolve_active_provenance(session)
    assert p.data_version == "2026.07.01"
    assert p.data_sync_run_id == rid


def test_returns_empty_when_no_successful_run():
    session = MagicMock()
    chain = session.query.return_value.filter.return_value.order_by.return_value
    chain.first.return_value = None
    p = resolve_active_provenance(session)
    assert p.data_version is None
    assert p.data_sync_run_id is None
