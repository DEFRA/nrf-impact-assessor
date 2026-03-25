"""Tests for the POST /check-boundary endpoint."""

import json
from io import BytesIO
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from app.main import app
from tests.unit.api.conftest import _make_geojson_bytes


@pytest.fixture
def client():
    return TestClient(app)


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
    def test_valid_geojson_returns_polygon_geometry(self, client):
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
        assert body["boundaryGeometryWgs84"]["type"] == "Polygon"
        assert len(body["boundaryGeometryWgs84"]["coordinates"]) >= 1

    @patch("app.boundary.router._find_intersecting_edps", _mock_no_edp_intersections)
    def test_properties_are_not_included(self, client):
        """User-supplied properties should not be present in bare geometry output."""
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
        geom = response.json()["boundaryGeometryWgs84"]
        assert set(geom.keys()) == {"type", "coordinates"}

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
        assert response.json()["boundaryGeometryWgs84"]["type"] == "Polygon"

    @patch("app.boundary.router._find_intersecting_edps", _mock_no_edp_intersections)
    def test_multiple_features_returns_first_polygon(self, client):
        """When input has multiple features, only the first polygon is returned."""
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
        body = response.json()
        # Only the first polygon should be returned
        assert body["boundaryGeometryWgs84"]["type"] == "Polygon"
        assert body["boundaryGeometryOriginal"]["type"] == "Polygon"

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

    def test_self_intersecting_polygon_returns_400_with_geometry(self, client):
        """A bowtie/figure-of-8 polygon should be rejected but include parsed geometry."""
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
        body = response.json()
        assert "invalid geometry" in body["error"].lower()
        assert body["geometry"]["type"] == "FeatureCollection"
        assert len(body["geometry"]["features"]) == 1

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
    """Tests for output projection."""

    @patch("app.boundary.router._find_intersecting_edps", _mock_no_edp_intersections)
    def test_bng_input_reprojected_to_wgs84(self, client):
        """BNG input geometry should be reprojected to WGS84 in boundaryGeometryWgs84."""
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
        coords = body["boundaryGeometryWgs84"]["coordinates"][0]
        for lng, lat in coords:
            assert -180 <= lng <= 180, f"longitude {lng} out of WGS84 range"
            assert -90 <= lat <= 90, f"latitude {lat} out of WGS84 range"

    @patch("app.boundary.router._find_intersecting_edps", _mock_no_edp_intersections)
    def test_original_geometry_preserves_input_crs(self, client):
        """boundaryGeometryOriginal should keep the input CRS (BNG)."""
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
        original = body["boundaryGeometryOriginal"]
        assert original["type"] == "Polygon"
        coords = original["coordinates"][0]
        for e, n in coords:
            assert abs(e) > 180 or abs(n) > 180, "Expected BNG coordinates"
        assert original["crs"]["type"] == "name"
        assert "27700" in original["crs"]["properties"]["name"]


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
        assert body["intersectingEdps"] == []

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
        assert len(body["intersectingEdps"]) == 2
        assert body["intersectingEdps"][0]["label"] == "Norfolk EDP 1"
        assert body["intersectingEdps"][1]["label"] == "Norfolk EDP 2"
        assert body["intersectingEdps"][0]["overlap_area_ha"] == pytest.approx(0.5)
        assert body["intersectingEdps"][0]["overlap_percentage"] == pytest.approx(25.0)
        assert body["intersectingEdps"][0]["intersection_geometry"]["type"] == "Polygon"
        assert body["intersectingEdps"][0]["edp_geometry"]["type"] == "Polygon"

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
        assert set(body.keys()) == {
            "boundaryGeometryOriginal",
            "boundaryGeometryWgs84",
            "intersectingEdps",
        }
