"""local_nutrient_assessment reads shapefile / GeoJSON input and reprojects to BNG."""

import zipfile

import geopandas as gpd
import pytest
import typer
from local_nutrient_assessment import _file_to_gdf
from shapely.geometry import Polygon

_BNG_SQUARE = Polygon(
    [(620000, 310000), (620500, 310000), (620500, 310500), (620000, 310500)]
)


def _bng_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame({"ref": ["a"]}, geometry=[_BNG_SQUARE], crs="EPSG:27700")


def test_geojson_in_wgs84_is_reprojected_to_bng(tmp_path):
    path = tmp_path / "site.geojson"
    _bng_gdf().to_crs("EPSG:4326").to_file(path, driver="GeoJSON")

    gdf = _file_to_gdf(path, crs="EPSG:27700")

    assert gdf.crs.to_epsg() == 27700
    assert gdf.geometry.iloc[0].area == pytest.approx(_BNG_SQUARE.area, rel=1e-3)


def test_shapefile_is_read(tmp_path):
    path = tmp_path / "site.shp"
    _bng_gdf().to_file(path)

    gdf = _file_to_gdf(path, crs="EPSG:27700")

    assert gdf.crs.to_epsg() == 27700
    assert gdf.geometry.iloc[0].equals(_BNG_SQUARE)


def test_zipped_shapefile_is_read(tmp_path):
    shp_dir = tmp_path / "shp"
    shp_dir.mkdir()
    _bng_gdf().to_file(shp_dir / "site.shp")
    zip_path = tmp_path / "site.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        for part in shp_dir.iterdir():
            zf.write(part, arcname=part.name)

    gdf = _file_to_gdf(zip_path, crs="EPSG:27700")

    assert gdf.crs.to_epsg() == 27700
    assert gdf.geometry.iloc[0].equals(_BNG_SQUARE)


def test_macos_finder_zip_with_subfolder_and_resource_forks_is_read(tmp_path):
    shp_dir = tmp_path / "shp"
    shp_dir.mkdir()
    _bng_gdf().to_file(shp_dir / "site.shp")
    zip_path = tmp_path / "site.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        for part in shp_dir.iterdir():
            zf.write(part, arcname=f"site/{part.name}")
            zf.writestr(f"__MACOSX/site/._{part.name}", b"\x00\x05\x16\x07")

    gdf = _file_to_gdf(zip_path, crs="EPSG:27700")

    assert gdf.crs.to_epsg() == 27700
    assert gdf.geometry.iloc[0].equals(_BNG_SQUARE)


def test_zip_without_shp_or_geojson_exits(tmp_path):
    zip_path = tmp_path / "site.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("readme.txt", "nothing here")

    with pytest.raises(typer.Exit):
        _file_to_gdf(zip_path, crs="EPSG:27700")


@pytest.mark.filterwarnings("ignore:'crs' was not provided")
def test_file_without_crs_uses_crs_option(tmp_path):
    path = tmp_path / "site.shp"
    _bng_gdf().set_crs(None, allow_override=True).to_file(path)

    gdf = _file_to_gdf(path, crs="EPSG:27700")

    assert gdf.crs.to_epsg() == 27700


def test_unsupported_suffix_exits(tmp_path):
    path = tmp_path / "site.kml"
    path.write_text("")

    with pytest.raises(typer.Exit):
        _file_to_gdf(path, crs="EPSG:27700")


def test_missing_file_exits(tmp_path):
    missing = tmp_path / "nope.geojson"

    with pytest.raises(typer.Exit):
        _file_to_gdf(missing, crs="EPSG:27700")
