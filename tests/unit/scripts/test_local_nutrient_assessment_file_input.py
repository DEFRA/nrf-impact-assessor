"""local_nutrient_assessment reads shapefile / GeoJSON input and reprojects to BNG."""

import zipfile

import geopandas as gpd
import pandas as pd
import pytest
import typer
from local_nutrient_assessment import _apply_job_fields, _file_to_gdf, _write_csv
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


def test_id_and_dwellings_fields_are_kept_case_insensitively(tmp_path):
    path = tmp_path / "site.shp"
    gpd.GeoDataFrame(
        {"ID": ["GNLP0337/0326"], "Dwellings": [50], "Other": ["x"]},
        geometry=[_BNG_SQUARE],
        crs="EPSG:27700",
    ).to_file(path)

    gdf = _file_to_gdf(path, crs="EPSG:27700")

    assert list(gdf.columns) == ["id", "dwellings", "geometry"]
    assert gdf["id"].iloc[0] == "GNLP0337/0326"
    assert gdf["dwellings"].iloc[0] == 50


def test_non_positive_dwellings_field_exits(tmp_path):
    path = tmp_path / "site.shp"
    gpd.GeoDataFrame(
        {"Dwellings": [0]}, geometry=[_BNG_SQUARE], crs="EPSG:27700"
    ).to_file(path)

    with pytest.raises(typer.Exit):
        _file_to_gdf(path, crs="EPSG:27700")


def test_file_dwellings_and_ids_override_job_fields():
    gdf = gpd.GeoDataFrame(
        {"id": ["A", "B"], "dwellings": [5, 7]},
        geometry=[_BNG_SQUARE, _BNG_SQUARE],
        crs="EPSG:27700",
    )

    result = _apply_job_fields(gdf, "job", "Dev", "house", 10)

    assert result["id"].tolist() == ["A", "B"]
    assert result["dwellings"].tolist() == [5, 7]


def test_job_fields_used_when_file_has_no_id_or_dwellings():
    result = _apply_job_fields(_bng_gdf()[["geometry"]], "job", "Dev", "house", 10)

    assert result["id"].tolist() == ["job"]
    assert result["dwellings"].tolist() == [10]


def test_csv_written_per_id_with_safe_unique_filenames(tmp_path):
    summary = pd.DataFrame({"id": ["GNLP0337/0326", "A", "A"], "value": [1, 2, 3]})

    _write_csv({"impact_summary": summary}, tmp_path, "job", has_file_ids=True)

    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "A.csv",
        "GNLP0337_0326.csv",
    ]
    assert pd.read_csv(tmp_path / "A.csv")["value"].tolist() == [2, 3]


def test_csv_named_after_job_id_without_file_ids(tmp_path):
    summary = pd.DataFrame({"id": ["job"], "value": [1]})

    _write_csv({"impact_summary": summary}, tmp_path, "local-run", has_file_ids=False)

    assert [p.name for p in tmp_path.iterdir()] == ["local-run.csv"]


def test_csv_filename_collisions_never_overwrite(tmp_path):
    summary = pd.DataFrame({"id": ["A/B", "A_B", "A_B_2"], "value": [1, 2, 3]})

    _write_csv({"impact_summary": summary}, tmp_path, "job", has_file_ids=True)

    written = sorted(p.name for p in tmp_path.iterdir())
    assert len(written) == 3
    values = sorted(pd.read_csv(tmp_path / name)["value"].item() for name in written)
    assert values == [1, 2, 3]
