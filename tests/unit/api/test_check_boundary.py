"""Tests for the POST /check-boundary endpoint."""

import json
from io import BytesIO

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


class TestCheckBoundaryGeoJSON:
    """Tests for POST /check-boundary with GeoJSON files."""

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
        assert body["type"] == "FeatureCollection"
        assert len(body["features"]) == 1
        assert body["features"][0]["geometry"]["type"] == "Polygon"

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
        assert response.json()["type"] == "FeatureCollection"

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
        assert len(response.json()["features"]) == 2

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
