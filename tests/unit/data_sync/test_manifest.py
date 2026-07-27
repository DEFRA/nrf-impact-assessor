import pytest
from pydantic import ValidationError

from app.data_sync.manifest import Manifest, TableEntry


def test_subset_of_one_table_is_valid():
    m = Manifest(tables={"lpa_boundaries": {"key": "k/1", "version": "20260724_1"}})
    assert m.tables["lpa_boundaries"] == TableEntry(key="k/1", version="20260724_1")


def test_multiple_tables_valid():
    m = Manifest(
        tables={
            "coefficient_layer": {"key": "a/1", "version": "v1"},
            "nn_catchments": {"key": "b/1", "version": "v1"},
        }
    )
    assert m.tables["nn_catchments"].key == "b/1"


def test_empty_tables_rejected():
    with pytest.raises(ValidationError, match="tables"):
        Manifest(tables={})


def test_entry_requires_non_empty_key_and_version():
    with pytest.raises(ValidationError):
        Manifest(tables={"lpa_boundaries": {"key": "", "version": "v"}})
    with pytest.raises(ValidationError):
        Manifest(tables={"lpa_boundaries": {"key": "k", "version": ""}})


def test_top_level_data_version_is_not_a_field():
    assert "data_version" not in Manifest.model_fields


def test_key_accepts_ordered_part_list():
    m = Manifest(
        tables={
            "nn_catchments": {
                "key": ["d/nn.sql.gz.part-aa", "d/nn.sql.gz.part-ab"],
                "version": "v1",
            }
        }
    )
    entry = m.tables["nn_catchments"]
    assert entry.keys == ["d/nn.sql.gz.part-aa", "d/nn.sql.gz.part-ab"]
    assert entry.is_split is True


def test_single_key_exposes_one_element_keys():
    entry = TableEntry(key="d/nn.sql.gz", version="v1")
    assert entry.keys == ["d/nn.sql.gz"]
    assert entry.is_split is False


def test_single_element_list_is_not_split():
    entry = TableEntry(key=["d/nn.sql.gz"], version="v1")
    assert entry.keys == ["d/nn.sql.gz"]
    assert entry.is_split is False


def test_empty_key_list_rejected():
    with pytest.raises(ValidationError, match="non-empty"):
        Manifest(tables={"nn_catchments": {"key": [], "version": "v1"}})


def test_empty_element_in_key_list_rejected():
    with pytest.raises(ValidationError, match="non-empty"):
        Manifest(
            tables={"nn_catchments": {"key": ["d/a.part-aa", ""], "version": "v1"}}
        )


def test_non_contiguous_part_suffixes_rejected():
    with pytest.raises(ValidationError, match="contiguous"):
        Manifest(
            tables={
                "nn_catchments": {
                    "key": ["d/nn.sql.gz.part-aa", "d/nn.sql.gz.part-ac"],
                    "version": "v1",
                }
            }
        )


def test_out_of_order_part_suffixes_rejected():
    with pytest.raises(ValidationError, match="contiguous"):
        Manifest(
            tables={
                "nn_catchments": {
                    "key": ["d/nn.sql.gz.part-ab", "d/nn.sql.gz.part-aa"],
                    "version": "v1",
                }
            }
        )


def test_part_suffix_rollover_accepted():
    """split's alphabet rolls az -> ba; that is contiguous, not a gap."""
    keys = [f"d/nn.sql.gz.part-a{c}" for c in "yz"] + ["d/nn.sql.gz.part-ba"]
    entry = TableEntry(key=keys, version="v1")
    assert entry.keys == keys


def test_non_part_names_skip_the_contiguity_guard():
    """Alternative naming schemes are not constrained; order is taken as given."""
    keys = ["d/chunk-3.gz", "d/chunk-1.gz"]
    entry = TableEntry(key=keys, version="v1")
    assert entry.keys == keys


def test_mixed_part_and_non_part_names_skip_the_guard():
    keys = ["d/nn.sql.gz.part-aa", "d/extra.gz"]
    assert TableEntry(key=keys, version="v1").keys == keys
