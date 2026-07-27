from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from app.data_sync import service
from app.data_sync.manifest import Manifest


def test_restore_all_rejects_table_not_in_allow_list():
    cfg = MagicMock()
    cfg.tables = ["nn_catchments", "coefficient_layer"]
    manifest = Manifest(tables={"not_a_table": {"key": "k1", "version": "v1"}})
    session = MagicMock()
    s3 = MagicMock()
    settings = MagicMock()

    with pytest.raises(ValueError, match="not in the data-sync allow-list"):
        service._restore_all(
            session, s3, cfg, settings, "eu-west-2", None, manifest, force=True
        )


def test_restore_all_records_failed_history_row_per_selected_table_on_qc_failure(
    monkeypatch,
):
    cfg = MagicMock()
    cfg.tables = ["nn_catchments", "coefficient_layer"]
    manifest = Manifest(
        tables={
            "nn_catchments": {"key": "k1.gz", "version": "v1"},
            "coefficient_layer": {"key": "k2.gz", "version": "v1"},
        },
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
    # force=True selects every table without consulting the DB for applied state.
    with pytest.raises(RuntimeError):
        service._restore_all(
            session, s3, cfg, settings, "eu-west-2", run_id, manifest, force=True
        )

    added = [call.args[0] for call in session.add.call_args_list]
    statuses = {row.table_name: row.status for row in added}
    assert statuses == {"nn_catchments": "failed", "coefficient_layer": "failed"}
    nn_row = next(row for row in added if row.table_name == "nn_catchments")
    assert "staged row count is 0" in nn_row.status_detail
    assert nn_row.data_version == "v1"
    other_row = next(row for row in added if row.table_name == "coefficient_layer")
    assert "blocked by QC failure on other table" in other_row.status_detail


def test_restore_all_aggregates_multiple_qc_failures_for_same_table(monkeypatch):
    """A table can fail multiple independent QC rules in the same run (the QC
    gate aggregates every failure before raising, rather than failing fast).
    All of them must land in status_detail — not just the last one parsed.
    """
    cfg = MagicMock()
    cfg.tables = ["nn_catchments"]
    manifest = Manifest(tables={"nn_catchments": {"key": "k1.gz", "version": "v1"}})
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
        service._restore_all(
            session, s3, cfg, settings, "eu-west-2", run_id, manifest, force=True
        )

    added = [call.args[0] for call in session.add.call_args_list]
    nn_row = next(row for row in added if row.table_name == "nn_catchments")
    assert "key_not_null" in nn_row.status_detail
    assert "non_null" in nn_row.status_detail


def test_recorded_key_single_is_unchanged():
    from app.data_sync.manifest import TableEntry
    from app.data_sync.service import recorded_key

    entry = TableEntry(key="20260727/nn.sql.gz", version="v1")
    assert recorded_key(entry) == "20260727/nn.sql.gz"


def test_recorded_key_split_uses_base_and_count():
    from app.data_sync.manifest import TableEntry
    from app.data_sync.service import recorded_key

    entry = TableEntry(
        key=[
            "20260727/nn.sql.gz.part-aa",
            "20260727/nn.sql.gz.part-ab",
            "20260727/nn.sql.gz.part-ac",
        ],
        version="v1",
    )
    assert recorded_key(entry) == "20260727/nn.sql.gz [3 parts]"


def test_recorded_key_split_without_part_suffix_falls_back_to_first_key():
    from app.data_sync.manifest import TableEntry
    from app.data_sync.service import recorded_key

    entry = TableEntry(key=["d/chunk-1.gz", "d/chunk-2.gz"], version="v1")
    assert recorded_key(entry) == "d/chunk-1.gz [2 parts]"


def test_recorded_etag_single_is_the_raw_etag():
    from app.data_sync.service import recorded_etag

    assert recorded_etag(["d41d8cd98f00b204e9800998ecf8427e"]) == (
        "d41d8cd98f00b204e9800998ecf8427e"
    )


def test_recorded_etag_composite_is_deterministic_and_order_sensitive():
    from app.data_sync.service import recorded_etag

    a = recorded_etag(["aaa", "bbb", "ccc"])
    assert a == recorded_etag(["aaa", "bbb", "ccc"])
    assert a != recorded_etag(["ccc", "bbb", "aaa"])
    assert a != recorded_etag(["aaa", "bbb", "ccd"])
    assert len(a) == 32


def test_part_dest_names_are_unique_per_key():
    """Part keys sharing a basename under different prefixes must not collide
    on local disk, or the restore reads one part twice and silently corrupts
    the stream. `.part-*` names differ naturally; custom schemes need not.
    """
    from app.data_sync.service import part_dest_names

    keys = ["chunk-1/data.gz", "chunk-2/data.gz", "chunk-3/data.gz"]
    names = part_dest_names(keys)
    assert len(set(names)) == 3
    assert all("data.gz" in n for n in names)  # basename still recognisable


def test_single_key_dest_name_is_the_bare_basename():
    """The single-key path keeps its existing on-disk name."""
    from app.data_sync.service import part_dest_names

    assert part_dest_names(["20260727/nn_catchments.sql.gz"]) == [
        "nn_catchments.sql.gz"
    ]


def test_part_dest_names_preserve_order():
    from app.data_sync.service import part_dest_names

    keys = [f"d/nn.sql.gz.part-a{c}" for c in "abc"]
    names = part_dest_names(keys)
    assert len(set(names)) == 3
    assert [n.endswith(("part-aa", "part-ab", "part-ac")) for n in names] == [
        True,
        True,
        True,
    ]
