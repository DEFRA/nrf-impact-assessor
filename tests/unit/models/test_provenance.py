"""Unit tests for DataProvenance / TableProvenance (DM-3)."""

from uuid import uuid4

from app.models.domain import DataProvenance, TableProvenance


def test_table_provenance_holds_version_and_run_id():
    rid = uuid4()
    tp = TableProvenance(data_version="2026.07.01", data_sync_run_id=rid)
    assert tp.data_version == "2026.07.01"
    assert tp.data_sync_run_id == rid


def test_provenance_holds_per_table_lineage():
    rid = uuid4()
    p = DataProvenance(
        tables={
            "nn_catchments": TableProvenance(data_version="A", data_sync_run_id=rid)
        }
    )
    assert p.tables["nn_catchments"].data_version == "A"
    assert p.tables["nn_catchments"].data_sync_run_id == rid


def test_provenance_defaults_to_empty_map():
    p = DataProvenance()
    assert p.tables == {}
