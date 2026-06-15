import pytest
from pydantic import ValidationError

from app.data_sync.manifest import Manifest


def test_valid_manifest():
    m = Manifest(
        data_version="20260521_074218",
        tables={
            "coefficient_layer": "public_coefficient_layer_20260521_074218.sql.gz",
            "nn_catchments": "public_nn_catchments_20260521_074218.sql.gz",
        },
    )
    assert m.data_version == "20260521_074218"
    assert m.tables["nn_catchments"].endswith(".sql.gz")


def test_rejects_empty_tables():
    with pytest.raises(ValidationError, match="tables"):
        Manifest(data_version="v1", tables={})


def test_rejects_missing_version():
    with pytest.raises(ValidationError, match="data_version"):
        Manifest(tables={"nn_catchments": "x.sql.gz"})


def test_rejects_missing_tables_key():
    with pytest.raises(ValidationError, match="tables"):
        Manifest(data_version="v1")


def test_rejects_empty_data_version():
    with pytest.raises(ValidationError, match="data_version"):
        Manifest(data_version="", tables={"nn_catchments": "x.sql.gz"})
