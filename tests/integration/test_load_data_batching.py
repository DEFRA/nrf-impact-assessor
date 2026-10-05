"""A batched load writes every feature, and stays all-or-nothing.

Reading in batches means several INSERTs where there used to be one, so the
DELETE and every batch must share a transaction: a bad geometry in a late
batch has to leave the table exactly as it was.
"""

from pathlib import Path

import geopandas as gpd
import pytest
from load_data import SpatialDataLoader
from shapely.geometry import Polygon
from sqlalchemy import func, select

from app.models.db import EdpExcludedAreas
from app.repositories.repository import Repository

pytestmark = pytest.mark.integration


def _square(x: int) -> Polygon:
    return Polygon([(x, 0), (x, 1), (x + 1, 1), (x + 1, 0)])


def _bowtie(x: int) -> Polygon:
    """Self-intersecting polygon — rejected by _check_geometry_validity."""
    return Polygon([(x, 0), (x + 2, 2), (x + 2, 0), (x, 2)])


def _write_gpkg(path: Path, geoms: list[Polygon]) -> Path:
    gdf = gpd.GeoDataFrame(
        {"site_name": [f"site-{i}" for i in range(len(geoms))]},
        geometry=geoms,
        crs="EPSG:27700",
    )
    gdf.to_file(path, layer="layer", driver="GPKG")
    return path


def _loader(repository: Repository, batch_size: int) -> SpatialDataLoader:
    fixtures_dir = Path(__file__).resolve().parents[1] / "data" / "fixtures"
    return SpatialDataLoader(
        repository, fixtures_dir=fixtures_dir, batch_size=batch_size
    )


def _load(loader: SpatialDataLoader, source: Path) -> None:
    loader._load_spatial_layer(
        layer_name="edp_excluded_areas",
        model=EdpExcludedAreas,
        file_path=source,
        layer="layer",
        name_column="site_name",
    )


def _count(repository: Repository) -> int:
    with repository.session() as session:
        return session.scalar(select(func.count()).select_from(EdpExcludedAreas))


def test_batched_load_writes_every_feature(repository: Repository, tmp_path: Path):
    source = _write_gpkg(tmp_path / "five.gpkg", [_square(i) for i in range(5)])

    _load(_loader(repository, batch_size=2), source)

    with repository.session() as session:
        names = session.scalars(select(EdpExcludedAreas.name)).all()
    assert sorted(names) == [f"site-{i}" for i in range(5)]


def test_invalid_geometry_in_a_later_batch_rolls_back_the_whole_load(
    repository: Repository, tmp_path: Path
):
    good = _write_gpkg(tmp_path / "good.gpkg", [_square(i) for i in range(4)])
    _load(_loader(repository, batch_size=2), good)
    assert _count(repository) == 4

    bad = _write_gpkg(
        tmp_path / "bad.gpkg", [_square(0), _square(1), _square(2), _bowtie(3)]
    )
    loader = _loader(repository, batch_size=2)
    with pytest.raises(ValueError, match="invalid or missing"):
        _load(loader, bad)

    # The previous load's rows survive: the DELETE rolled back with the inserts.
    with repository.session() as session:
        names = session.scalars(select(EdpExcludedAreas.name)).all()
    assert sorted(names) == [f"site-{i}" for i in range(4)]
