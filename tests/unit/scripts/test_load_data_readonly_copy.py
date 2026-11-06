"""Reference data is read from a private scratch copy, so the copy must be
readable for every source format the loader accepts."""

import geopandas as gpd
from load_data import _gdal_readonly
from shapely.geometry import Polygon


def _write_layer(path, driver: str) -> None:
    gpd.GeoDataFrame(
        {"name": ["a"], "geometry": [Polygon([(0, 0), (0, 1), (1, 1)])]},
        crs="EPSG:27700",
    ).to_file(path, driver=driver)


def test_shapefile_copy_includes_its_sidecar_files(tmp_path):
    source = tmp_path / "layer.shp"
    _write_layer(source, "ESRI Shapefile")

    with _gdal_readonly(source) as scratch_path:
        gdf = gpd.read_file(scratch_path)

    assert gdf["name"].tolist() == ["a"]
    assert gdf.crs.to_epsg() == 27700


def test_geopackage_copy_is_readable(tmp_path):
    source = tmp_path / "layer.gpkg"
    _write_layer(source, "GPKG")

    with _gdal_readonly(source) as scratch_path:
        gdf = gpd.read_file(scratch_path)

    assert gdf["name"].tolist() == ["a"]


def test_unrelated_files_beside_the_source_are_not_copied(tmp_path):
    source = tmp_path / "layer.shp"
    _write_layer(source, "ESRI Shapefile")
    _write_layer(tmp_path / "other.gpkg", "GPKG")

    with _gdal_readonly(source) as scratch_path:
        copied = sorted(p.name for p in scratch_path.parent.iterdir())

    assert "other.gpkg" not in copied
    assert {"layer.shp", "layer.shx", "layer.dbf", "layer.prj"} <= set(copied)
