"""Tests for the POST /check-boundary endpoint."""

import json
from io import BytesIO
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture
def client():
    return TestClient(app)


def _make_geojson_bytes(
    coordinates: list | None = None,
    crs: str | None = None,
) -> bytes:
    """Create a minimal GeoJSON FeatureCollection as bytes."""
    if coordinates is None:
        coordinates = [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]

    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": coordinates,
                },
                "properties": {"name": "test"},
            }
        ],
    }
    if crs:
        geojson["crs"] = {
            "type": "name",
            "properties": {"name": crs},
        }
    return json.dumps(geojson).encode()


def _mock_no_edp_intersections(gdf, repository, output_srid=4326):
    """Mock that returns no intersecting EDPs."""
    return []


def _mock_edp_intersections(gdf, repository, output_srid=4326):
    """Mock that returns intersecting EDPs."""
    return [
        {
            "label": "Norfolk EDP 1",
            "n2k_site_name": "Site A",
            "edp_geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-1.6, 51.9],
                        [-1.3, 51.9],
                        [-1.3, 52.2],
                        [-1.6, 52.2],
                        [-1.6, 51.9],
                    ]
                ],
            },
            "intersection_geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-1.5, 52.0],
                        [-1.4, 52.0],
                        [-1.4, 52.1],
                        [-1.5, 52.1],
                        [-1.5, 52.0],
                    ]
                ],
            },
            "overlap_area_ha": 0.5,
            "overlap_area_sqm": 5000.0,
            "overlap_percentage": 25.0,
        },
        {
            "label": "Norfolk EDP 2",
            "n2k_site_name": "Site B",
            "edp_geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-1.4, 51.9],
                        [-1.1, 51.9],
                        [-1.1, 52.2],
                        [-1.4, 52.2],
                        [-1.4, 51.9],
                    ]
                ],
            },
            "intersection_geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-1.3, 52.0],
                        [-1.2, 52.0],
                        [-1.2, 52.1],
                        [-1.3, 52.1],
                        [-1.3, 52.0],
                    ]
                ],
            },
            "overlap_area_ha": 0.3,
            "overlap_area_sqm": 3000.0,
            "overlap_percentage": 15.0,
        },
    ]


class TestCheckBoundaryGeoJSON:
    """Tests for POST /check-boundary with GeoJSON files."""

    @patch("app.boundary.router._find_intersecting_edps", _mock_no_edp_intersections)
    def test_valid_geojson_returns_feature_collection(self, client):
        content = _make_geojson_bytes()
        response = client.post(
            "/check-boundary",
            files={
                "geometry_file": (
                    "boundary.geojson",
                    BytesIO(content),
                    "application/json",
                )
            },
        )

        assert response.status_code == 200
        body = response.json()
        assert body["geometry"]["type"] == "FeatureCollection"
        assert len(body["geometry"]["features"]) == 1
        assert body["geometry"]["features"][0]["geometry"]["type"] == "Polygon"

    @patch("app.boundary.router._find_intersecting_edps", _mock_no_edp_intersections)
    def test_properties_are_stripped_from_features(self, client):
        """User-supplied properties should be removed to avoid leaking PII."""
        content = _make_geojson_bytes()
        response = client.post(
            "/check-boundary",
            files={
                "geometry_file": (
                    "boundary.geojson",
                    BytesIO(content),
                    "application/json",
                )
            },
        )

        assert response.status_code == 200
        feature = response.json()["geometry"]["features"][0]
        assert feature["properties"] == {}

    @patch("app.boundary.router._find_intersecting_edps", _mock_no_edp_intersections)
    def test_json_extension_accepted(self, client):
        content = _make_geojson_bytes()
        response = client.post(
            "/check-boundary",
            files={
                "geometry_file": (
                    "boundary.json",
                    BytesIO(content),
                    "application/json",
                )
            },
        )

        assert response.status_code == 200
        assert response.json()["geometry"]["type"] == "FeatureCollection"

    @patch("app.boundary.router._find_intersecting_edps", _mock_no_edp_intersections)
    def test_multiple_features_returned(self, client):
        geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
                    },
                    "properties": {"id": 1},
                },
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[2, 2], [3, 2], [3, 3], [2, 3], [2, 2]]],
                    },
                    "properties": {"id": 2},
                },
            ],
        }
        content = json.dumps(geojson).encode()
        response = client.post(
            "/check-boundary",
            files={
                "geometry_file": (
                    "multi.geojson",
                    BytesIO(content),
                    "application/json",
                )
            },
        )

        assert response.status_code == 200
        assert len(response.json()["geometry"]["features"]) == 2

    def test_invalid_geojson_returns_400(self, client):
        response = client.post(
            "/check-boundary",
            files={
                "geometry_file": (
                    "bad.geojson",
                    BytesIO(b"not valid json"),
                    "application/json",
                )
            },
        )

        assert response.status_code == 400
        assert "Failed to read geometry file" in response.json()["detail"]

    def test_unsupported_format_returns_400(self, client):
        response = client.post(
            "/check-boundary",
            files={
                "geometry_file": (
                    "data.csv",
                    BytesIO(b"col1,col2\n1,2"),
                    "text/csv",
                )
            },
        )

        assert response.status_code == 400
        assert "Unsupported file format" in response.json()["detail"]

    def test_file_too_large_returns_413(self, client):
        from app.boundary.router import _max_upload_bytes

        content = b"x" * (_max_upload_bytes + 1)
        response = client.post(
            "/check-boundary",
            files={
                "geometry_file": (
                    "huge.geojson",
                    BytesIO(content),
                    "application/json",
                )
            },
        )

        assert response.status_code == 413
        assert "File too large" in response.json()["detail"]

    def test_shapefile_without_crs_returns_422(self, client):
        """A .shp without a .prj has no CRS — should return 422 with helpful message."""
        import tempfile
        import zipfile
        from pathlib import Path

        import geopandas as gpd
        from shapely.geometry import Polygon

        # Create a shapefile without a .prj file
        gdf = gpd.GeoDataFrame(
            {"id": [1]},
            geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
            crs=None,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            shp_path = Path(tmpdir) / "no_crs.shp"
            gdf.to_file(shp_path)
            # Remove .prj to ensure no CRS
            for prj in Path(tmpdir).glob("*.prj"):
                prj.unlink()

            # Zip the shapefile components
            zip_buf = BytesIO()
            with zipfile.ZipFile(zip_buf, "w") as zf:
                for f in Path(tmpdir).glob("no_crs.*"):
                    zf.write(f, f.name)
            zip_buf.seek(0)

        response = client.post(
            "/check-boundary",
            files={
                "geometry_file": (
                    "no_crs.zip",
                    zip_buf,
                    "application/zip",
                )
            },
        )

        assert response.status_code == 422
        detail = response.json()["detail"]
        assert "coordinate reference system" in detail.lower()
        assert ".prj" in detail
        assert "Please ensure your boundary file" in detail

    def test_shapefile_zip_missing_companion_files_returns_400(self, client):
        """A zip with only .shp (no .dbf/.shx) should return 400."""
        import zipfile

        # Create a zip containing only a .shp file (no companions)
        zip_buf = BytesIO()
        with zipfile.ZipFile(zip_buf, "w") as zf:
            zf.writestr("boundary.shp", b"fake shapefile content")
        zip_buf.seek(0)

        response = client.post(
            "/check-boundary",
            files={
                "geometry_file": (
                    "incomplete.zip",
                    zip_buf,
                    "application/zip",
                )
            },
        )

        assert response.status_code == 400
        detail = response.json()["detail"]
        assert "missing required companion files" in detail
        assert ".dbf" in detail
        assert ".shx" in detail


class TestCheckBoundaryGeometryValidation:
    """Tests for geometry validation in POST /check-boundary."""

    def test_self_intersecting_polygon_returns_400(self, client):
        """A bowtie/figure-of-8 polygon should be rejected."""
        content = _make_geojson_bytes(
            coordinates=[[[0, 0], [1, 1], [1, 0], [0, 1], [0, 0]]]
        )
        response = client.post(
            "/check-boundary",
            files={
                "geometry_file": (
                    "self-intersecting.geojson",
                    BytesIO(content),
                    "application/json",
                )
            },
        )

        assert response.status_code == 400
        assert "invalid geometry" in response.json()["detail"].lower()

    @patch("app.boundary.router._find_intersecting_edps", _mock_no_edp_intersections)
    def test_valid_polygon_passes_validation(self, client):
        """A valid polygon should pass geometry validation."""
        content = _make_geojson_bytes(
            coordinates=[[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]
        )
        response = client.post(
            "/check-boundary",
            files={
                "geometry_file": (
                    "valid.geojson",
                    BytesIO(content),
                    "application/json",
                )
            },
        )

        assert response.status_code == 200


class TestCheckBoundaryProjection:
    """Tests for the proj query parameter."""

    @patch("app.boundary.router._find_intersecting_edps", _mock_no_edp_intersections)
    def test_proj_parameter_reprojects_output(self, client):
        """When proj=EPSG:4326 is passed, geometry should be returned in WGS84."""
        # Use BNG coordinates (EPSG:27700) with explicit CRS
        content = _make_geojson_bytes(
            coordinates=[
                [
                    [400000, 100000],
                    [400100, 100000],
                    [400100, 100100],
                    [400000, 100100],
                    [400000, 100000],
                ]
            ],
            crs="urn:ogc:def:crs:EPSG::27700",
        )
        response = client.post(
            "/check-boundary?proj=EPSG:4326",
            files={
                "geometry_file": (
                    "boundary.geojson",
                    BytesIO(content),
                    "application/json",
                )
            },
        )

        assert response.status_code == 200
        body = response.json()
        coords = body["geometry"]["features"][0]["geometry"]["coordinates"][0]
        # All coordinates should be WGS84 range (lng -180..180, lat -90..90)
        for lng, lat in coords:
            assert -180 <= lng <= 180, f"longitude {lng} out of WGS84 range"
            assert -90 <= lat <= 90, f"latitude {lat} out of WGS84 range"

    @patch("app.boundary.router._find_intersecting_edps", _mock_no_edp_intersections)
    def test_default_proj_returns_wgs84(self, client):
        """Without explicit proj parameter, geometry defaults to WGS84 (EPSG:4326)."""
        content = _make_geojson_bytes(
            coordinates=[
                [
                    [400000, 100000],
                    [400100, 100000],
                    [400100, 100100],
                    [400000, 100100],
                    [400000, 100000],
                ]
            ],
            crs="urn:ogc:def:crs:EPSG::27700",
        )
        response = client.post(
            "/check-boundary",
            files={
                "geometry_file": (
                    "boundary.geojson",
                    BytesIO(content),
                    "application/json",
                )
            },
        )

        assert response.status_code == 200
        body = response.json()
        coords = body["geometry"]["features"][0]["geometry"]["coordinates"][0]
        # Default is EPSG:4326 so coordinates should be WGS84 range
        for lng, lat in coords:
            assert -180 <= lng <= 180, f"longitude {lng} out of WGS84 range"
            assert -90 <= lat <= 90, f"latitude {lat} out of WGS84 range"

    @patch("app.boundary.router._find_intersecting_edps", _mock_no_edp_intersections)
    def test_proj_27700_returns_bng(self, client):
        """When proj=EPSG:27700 is passed, geometry stays in BNG."""
        content = _make_geojson_bytes(
            coordinates=[
                [
                    [400000, 100000],
                    [400100, 100000],
                    [400100, 100100],
                    [400000, 100100],
                    [400000, 100000],
                ]
            ],
            crs="urn:ogc:def:crs:EPSG::27700",
        )
        response = client.post(
            "/check-boundary?proj=EPSG:27700",
            files={
                "geometry_file": (
                    "boundary.geojson",
                    BytesIO(content),
                    "application/json",
                )
            },
        )

        assert response.status_code == 200
        body = response.json()
        coords = body["geometry"]["features"][0]["geometry"]["coordinates"][0]
        # Coordinates should be in BNG range (large values)
        for e, n in coords:
            assert abs(e) > 180 or abs(n) > 180, "Expected BNG coordinates"


class TestCheckBoundaryEdpIntersection:
    """Tests for EDP intersection logic in the response."""

    @patch("app.boundary.router._find_intersecting_edps", _mock_no_edp_intersections)
    def test_no_intersections_returns_empty_list(self, client):
        content = _make_geojson_bytes()
        response = client.post(
            "/check-boundary",
            files={
                "geometry_file": (
                    "boundary.geojson",
                    BytesIO(content),
                    "application/json",
                )
            },
        )

        assert response.status_code == 200
        body = response.json()
        assert body["intersects_edp"] is False
        assert body["intersecting_edps"] == []

    @patch("app.boundary.router._find_intersecting_edps", _mock_edp_intersections)
    def test_intersections_returns_edp_details(self, client):
        content = _make_geojson_bytes()
        response = client.post(
            "/check-boundary",
            files={
                "geometry_file": (
                    "boundary.geojson",
                    BytesIO(content),
                    "application/json",
                )
            },
        )

        assert response.status_code == 200
        body = response.json()
        assert body["intersects_edp"] is True
        assert len(body["intersecting_edps"]) == 2
        assert body["intersecting_edps"][0]["label"] == "Norfolk EDP 1"
        assert body["intersecting_edps"][1]["label"] == "Norfolk EDP 2"
        assert body["intersecting_edps"][0]["overlap_area_ha"] == pytest.approx(0.5)
        assert body["intersecting_edps"][0]["overlap_percentage"] == pytest.approx(25.0)
        assert (
            body["intersecting_edps"][0]["intersection_geometry"]["type"] == "Polygon"
        )
        assert body["intersecting_edps"][0]["edp_geometry"]["type"] == "Polygon"

    @patch("app.boundary.router._find_intersecting_edps", _mock_edp_intersections)
    def test_response_contains_all_expected_keys(self, client):
        content = _make_geojson_bytes()
        response = client.post(
            "/check-boundary",
            files={
                "geometry_file": (
                    "boundary.geojson",
                    BytesIO(content),
                    "application/json",
                )
            },
        )

        assert response.status_code == 200
        body = response.json()
        assert set(body.keys()) == {"geometry", "intersecting_edps", "intersects_edp"}
