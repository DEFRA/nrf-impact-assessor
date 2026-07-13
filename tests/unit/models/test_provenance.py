"""Unit tests for DataProvenance (DM-3)."""

from uuid import uuid4

from app.models.domain import DataProvenance


def test_provenance_holds_version_and_run_id():
    rid = uuid4()
    p = DataProvenance(data_version="2026.07.01", data_sync_run_id=rid)
    assert p.data_version == "2026.07.01"
    assert p.data_sync_run_id == rid


def test_provenance_defaults_are_none():
    p = DataProvenance()
    assert p.data_version is None
    assert p.data_sync_run_id is None
