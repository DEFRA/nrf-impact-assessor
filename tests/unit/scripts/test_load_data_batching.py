"""Reference layers are read in bounded batches, not one whole-file read.

The coefficient layer is 5.4M polygons; loading it as a single GeoDataFrame
needs more RAM than the machines that run this script have.
"""

from pathlib import Path

import geopandas as gpd
import load_data
import pytest
from load_data import SpatialDataLoader
from shapely.geometry import Polygon
from typer.testing import CliRunner


def _square(x: int) -> Polygon:
    """A unit square offset along x, so every feature is distinguishable."""
    return Polygon([(x, 0), (x, 1), (x + 1, 1), (x + 1, 0)])


@pytest.fixture
def five_feature_gpkg(tmp_path: Path) -> Path:
    gdf = gpd.GeoDataFrame(
        {"name": [f"feature-{i}" for i in range(5)]},
        geometry=[_square(i) for i in range(5)],
        crs="EPSG:27700",
    )
    path = tmp_path / "layer.gpkg"
    gdf.to_file(path, layer="layer", driver="GPKG")
    return path


def _names(batches) -> list[list[str]]:
    return [batch["name"].tolist() for batch in batches]


def test_batches_split_the_layer_into_chunks_of_batch_size(five_feature_gpkg: Path):
    batches = SpatialDataLoader._read_batches(
        five_feature_gpkg, layer="layer", batch_size=2
    )

    assert _names(batches) == [
        ["feature-0", "feature-1"],
        ["feature-2", "feature-3"],
        ["feature-4"],
    ]


def test_batch_size_zero_reads_the_whole_layer_at_once(five_feature_gpkg: Path):
    batches = SpatialDataLoader._read_batches(
        five_feature_gpkg, layer="layer", batch_size=0
    )

    assert _names(batches) == [[f"feature-{i}" for i in range(5)]]


def test_limit_stops_reading_once_enough_features_are_yielded(five_feature_gpkg: Path):
    """Sample mode must stop reading, not read everything then truncate."""
    batches = SpatialDataLoader._read_batches(
        five_feature_gpkg, layer="layer", batch_size=2, limit=3
    )

    assert _names(batches) == [["feature-0", "feature-1"], ["feature-2"]]


def test_count_features_does_not_read_the_geometries(five_feature_gpkg: Path):
    assert SpatialDataLoader._count_features(five_feature_gpkg, layer="layer") == 5


def test_batch_size_defaults_to_the_module_default():
    fixtures_dir = Path(__file__).resolve().parents[2] / "data" / "fixtures"
    loader = SpatialDataLoader(repository=None, fixtures_dir=fixtures_dir)

    assert loader.batch_size == load_data.DEFAULT_BATCH_SIZE


def test_cli_batch_size_option_reaches_the_loader(monkeypatch, tmp_path: Path):
    """--batch-size is threaded through to SpatialDataLoader."""
    captured = {}

    class _StubLoader:
        def __init__(self, repository, settings=None, **kwargs):
            captured.update(kwargs)

        def load_spatial_layers(self, layer_types=None):
            pass

    monkeypatch.setattr(load_data, "SpatialDataLoader", _StubLoader)
    monkeypatch.setattr(load_data, "create_db_engine", lambda _settings: None)
    monkeypatch.setattr(load_data, "db_settings", lambda: None)
    monkeypatch.setattr(load_data, "Repository", lambda _engine: _NullRepository())
    monkeypatch.setattr(load_data, "ScriptSettings", lambda: _StubSettings(tmp_path))

    result = CliRunner().invoke(
        load_data.app, ["--layer", "wwtw_catchments", "--batch-size", "7", "--yes"]
    )

    assert result.exit_code == 0, result.output
    assert captured["batch_size"] == 7


class _NullRepository:
    def close(self):
        pass


class _StubSettings:
    def __init__(self, base_path: Path):
        self.base_path = base_path


def test_cleaning_coefficient_columns_returns_null_counts():
    """Counts are returned, not printed, so a batched load can total them once
    instead of printing four lines per batch."""
    gdf = gpd.GeoDataFrame(
        {
            "lu_curr_n_coeff": ["1.5", "n/a", ""],
            "n_resi_coeff": ["0.1", "0.2", "0.3"],
        },
        geometry=[_square(i) for i in range(3)],
        crs="EPSG:27700",
    )

    counts = SpatialDataLoader._clean_coeff_columns(
        gdf, ["lu_curr_n_coeff", "n_resi_coeff", "absent_column"]
    )

    assert counts == {"lu_curr_n_coeff": 2, "n_resi_coeff": 0}
