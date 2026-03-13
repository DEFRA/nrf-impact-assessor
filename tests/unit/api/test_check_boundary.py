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


def _mock_no_edp_intersections(gdf, repository):
    """Mock that returns no intersecting EDPs."""
    return []


def _mock_edp_intersections(gdf, repository):
    """Mock that returns intersecting EDPs."""
    return [
        {"name": "Norfolk EDP 1", "attributes": {"region": "norfolk"}},
        {"name": "Norfolk EDP 2", "attributes": {"region": "norfolk"}},
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
        import geopandas as gpd
        import tempfile
        import zipfile
        from pathlib import Path
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
        assert body["intersecting_edps"][0]["name"] == "Norfolk EDP 1"
        assert body["intersecting_edps"][1]["name"] == "Norfolk EDP 2"

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
