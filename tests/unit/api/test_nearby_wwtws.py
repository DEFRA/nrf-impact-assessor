"""Tests for the POST /wwtw/nearby endpoint."""

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from app.main import app

# A simple BNG polygon (100m square near the centre of England)
_VALID_GEOMETRY = {
    "type": "Polygon",
    "coordinates": [
        [
            [400000, 300000],
            [400100, 300000],
            [400100, 300100],
            [400000, 300100],
            [400000, 300000],
        ]
    ],
}


@pytest.fixture
def client():
    return TestClient(app)


def _mock_nearby_multiple(rlb_wkt, repository, max_distance_m=10000.0, srid=4326):
    """Mock returning two nearby WWTWs at different distances."""
    return [
        {"wwtw_id": "101", "distance_m": 0.0},
        {"wwtw_id": "202", "distance_m": 5432.1},
    ]


def _mock_nearby_empty(rlb_wkt, repository, max_distance_m=10000.0, srid=4326):
    """Mock returning no nearby WWTWs."""
    return []


def _mock_nearby_single(rlb_wkt, repository, max_distance_m=10000.0, srid=4326):
    """Mock returning a single overlapping WWTW."""
    return [{"wwtw_id": "101", "distance_m": 0.0}]


_MOCK_LOOKUP_DATA = [
    {"wwtw_code": 101, "wwtw_name": "Great Billing WRC"},
    {"wwtw_code": 202, "wwtw_name": "Letchworth WWTP"},
]


def _mock_load_lookup(repository):
    """Mock the wwtw_lookup loader."""
    import pandas as pd

    return pd.DataFrame(_MOCK_LOOKUP_DATA)


class TestNearbyWwtws:
    """Tests for POST /wwtw/nearby."""

    @patch("app.wwtw.router._load_wwtw_lookup", _mock_load_lookup)
    @patch("app.wwtw.router._find_nearby_wwtws", _mock_nearby_multiple)
    def test_returns_multiple_wwtws_ordered_by_distance(self, client):
        response = client.post("/wwtw/nearby", json={"geometry": _VALID_GEOMETRY})

        assert response.status_code == 200
        body = response.json()
        items = body["nearbyWwtws"]
        assert len(items) == 2
        assert items[0]["wwtwId"] == "101"
        assert items[0]["wwtwName"] == "Great Billing WRC"
        assert items[0]["distanceKm"] == pytest.approx(0.0)
        assert items[1]["wwtwId"] == "202"
        assert items[1]["wwtwName"] == "Letchworth WWTP"
        assert items[1]["distanceKm"] == pytest.approx(5.4)

    @patch("app.wwtw.router._load_wwtw_lookup", _mock_load_lookup)
    @patch("app.wwtw.router._find_nearby_wwtws", _mock_nearby_empty)
    def test_returns_empty_list_when_no_nearby(self, client):
        response = client.post("/wwtw/nearby", json={"geometry": _VALID_GEOMETRY})

        assert response.status_code == 200
        body = response.json()
        assert body["nearbyWwtws"] == []

    @patch("app.wwtw.router._load_wwtw_lookup", _mock_load_lookup)
    @patch("app.wwtw.router._find_nearby_wwtws", _mock_nearby_single)
    def test_overlapping_catchment_returns_zero_distance(self, client):
        response = client.post("/wwtw/nearby", json={"geometry": _VALID_GEOMETRY})

        assert response.status_code == 200
        body = response.json()
        assert len(body["nearbyWwtws"]) == 1
        assert body["nearbyWwtws"][0]["distanceKm"] == pytest.approx(0.0)

    def test_invalid_geometry_returns_400(self, client):
        response = client.post(
            "/wwtw/nearby", json={"geometry": {"type": "Invalid", "coordinates": []}}
        )

        assert response.status_code == 400
        body = response.json()
        assert "detail" in body

    def test_missing_geometry_returns_422(self, client):
        response = client.post("/wwtw/nearby", json={})

        assert response.status_code == 422

    def test_empty_body_returns_422(self, client):
        response = client.post("/wwtw/nearby")

        assert response.status_code == 422

    @patch("app.wwtw.router._load_wwtw_lookup", _mock_load_lookup)
    @patch("app.wwtw.router._find_nearby_wwtws", _mock_nearby_multiple)
    def test_response_keys_are_camel_case(self, client):
        response = client.post("/wwtw/nearby", json={"geometry": _VALID_GEOMETRY})

        assert response.status_code == 200
        body = response.json()
        assert "nearbyWwtws" in body
        item = body["nearbyWwtws"][0]
        assert set(item.keys()) == {"wwtwId", "wwtwName", "distanceKm"}

    @patch("app.wwtw.router._load_wwtw_lookup", _mock_load_lookup)
    @patch("app.wwtw.router._find_nearby_wwtws", _mock_nearby_multiple)
    def test_unknown_wwtw_id_gets_fallback_name(self, client):
        """When a WWTW ID is not in the lookup, a fallback name is used."""

        def mock_nearby_unknown(rlb_wkt, repository, max_distance_m=10000.0, srid=4326):
            return [{"wwtw_id": "999", "distance_m": 1000.0}]

        with patch("app.wwtw.router._find_nearby_wwtws", mock_nearby_unknown):
            response = client.post("/wwtw/nearby", json={"geometry": _VALID_GEOMETRY})

        assert response.status_code == 200
        body = response.json()
        assert body["nearbyWwtws"][0]["wwtwName"] == "WWTW 999"
