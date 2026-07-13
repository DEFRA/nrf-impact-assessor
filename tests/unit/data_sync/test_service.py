from unittest.mock import MagicMock

import pytest

from app.data_sync import service
from app.data_sync.manifest import Manifest
from app.data_sync.service import needs_reload


def test_needs_reload_true_when_versions_differ():
    m = Manifest(data_version="v2", tables={"a": "a.gz"})
    assert needs_reload(m, applied_version="v1", force=False) is True


def test_needs_reload_false_when_versions_match():
    m = Manifest(data_version="v1", tables={"a": "a.gz"})
    assert needs_reload(m, applied_version="v1", force=False) is False


def test_needs_reload_true_when_forced():
    m = Manifest(data_version="v1", tables={"a": "a.gz"})
    assert needs_reload(m, applied_version="v1", force=True) is True


def test_needs_reload_true_when_nothing_applied():
    m = Manifest(data_version="v1", tables={"a": "a.gz"})
    assert needs_reload(m, applied_version=None, force=False) is True


def test_restore_all_rejects_manifest_missing_allow_listed_table(monkeypatch):
    cfg = MagicMock()
    cfg.tables = ["nn_catchments", "coefficient_layer"]
    manifest = Manifest(data_version="v1", tables={"nn_catchments": "k1"})
    session = MagicMock()
    s3 = MagicMock()
    settings = MagicMock()

    with pytest.raises(ValueError, match="missing required table"):
        service._restore_all(session, s3, cfg, settings, "eu-west-2", None, manifest)


def test_restore_all_records_failed_history_row_per_table_on_qc_failure(
    monkeypatch, tmp_path
):
    from uuid import uuid4

    cfg = MagicMock()
    cfg.tables = ["nn_catchments", "coefficient_layer"]
    manifest = Manifest(
        data_version="v1",
        tables={"nn_catchments": "k1.gz", "coefficient_layer": "k2.gz"},
    )
    session = MagicMock()
    s3 = MagicMock()
    s3.object_etag.return_value = "etag"

    def _fake_download(key, dest):
        dest.write_bytes(b"")

    s3.download_object.side_effect = _fake_download

    error_text = (
        "psql atomic restore failed: "
        "table=nn_catchments rule=row_count detail=staged row count is 0\n"
    )

    def _raise(*_args, **_kwargs):
        raise RuntimeError(error_text)

    monkeypatch.setattr(service, "restore_all_atomic", _raise)

    run_id = uuid4()
    settings = MagicMock()
    with pytest.raises(RuntimeError):
        service._restore_all(session, s3, cfg, settings, "eu-west-2", run_id, manifest)

    added = [call.args[0] for call in session.add.call_args_list]
    statuses = {row.table_name: row.status for row in added}
    assert statuses == {"nn_catchments": "failed", "coefficient_layer": "failed"}
    nn_row = next(row for row in added if row.table_name == "nn_catchments")
    assert "staged row count is 0" in nn_row.status_detail
    other_row = next(row for row in added if row.table_name == "coefficient_layer")
    assert "blocked by QC failure on other table" in other_row.status_detail


def test_restore_all_aggregates_multiple_qc_failures_for_same_table(
    monkeypatch, tmp_path
):
    """A table can fail multiple independent QC rules in the same run (the QC
    gate aggregates every failure before raising, rather than failing fast).
    All of them must land in status_detail — not just the last one parsed.
    """
    from uuid import uuid4

    cfg = MagicMock()
    cfg.tables = ["nn_catchments"]
    manifest = Manifest(
        data_version="v1",
        tables={"nn_catchments": "k1.gz"},
    )
    session = MagicMock()
    s3 = MagicMock()
    s3.object_etag.return_value = "etag"

    def _fake_download(key, dest):
        dest.write_bytes(b"")

    s3.download_object.side_effect = _fake_download

    error_text = (
        "psql atomic restore failed: "
        "table=nn_catchments rule=key_not_null detail=3 row(s) with NULL key\n"
        "table=nn_catchments rule=non_null detail=5 row(s) with NULL geom\n"
    )

    def _raise(*_args, **_kwargs):
        raise RuntimeError(error_text)

    monkeypatch.setattr(service, "restore_all_atomic", _raise)

    run_id = uuid4()
    settings = MagicMock()
    with pytest.raises(RuntimeError):
        service._restore_all(session, s3, cfg, settings, "eu-west-2", run_id, manifest)

    added = [call.args[0] for call in session.add.call_args_list]
    nn_row = next(row for row in added if row.table_name == "nn_catchments")
    assert "key_not_null" in nn_row.status_detail
    assert "non_null" in nn_row.status_detail
