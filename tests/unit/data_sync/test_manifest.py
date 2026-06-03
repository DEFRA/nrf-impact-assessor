import pytest

from app.data_sync.manifest import Manifest, parse_manifest


def test_parse_valid_manifest():
    raw = {
        "data_version": "20260521_074218",
        "tables": {
            "coefficient_layer": "public_coefficient_layer_20260521_074218.sql.gz",
            "nn_catchments": "public_nn_catchments_20260521_074218.sql.gz",
        },
    }
    m = parse_manifest(raw)
    assert isinstance(m, Manifest)
    assert m.data_version == "20260521_074218"
    assert m.tables["nn_catchments"].endswith(".sql.gz")


def test_parse_rejects_empty_tables():
    with pytest.raises(ValueError, match="tables"):
        parse_manifest({"data_version": "v1", "tables": {}})


def test_parse_rejects_missing_version():
    with pytest.raises(ValueError, match="data_version"):
        parse_manifest({"tables": {"nn_catchments": "x.sql.gz"}})
