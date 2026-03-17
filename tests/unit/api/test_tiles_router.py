"""Tests for GET /tiles/{layer}/{z}/{x}/{y}.mvt"""

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

import app.tiles.router as tiles_router_module
from app.main import app

MVT_CONTENT_TYPE = "application/vnd.mapbox-vector-tile"
FAKE_TILE = b"\x1a\x00"  # minimal non-empty bytes


@pytest.fixture(autouse=True)
def reset_tile_caches():
    """Clear module-level caches before each test for isolation."""
    tiles_router_module._tile_cache.clear()
    tiles_router_module._version_cache.clear()
    tiles_router_module._edp_version_cache = None
    yield
    tiles_router_module._tile_cache.clear()
    tiles_router_module._version_cache.clear()
    tiles_router_module._edp_version_cache = None


@pytest.fixture
def client():
    return TestClient(app)


def _mock_resolve_version(_layer_type):
    return 1


def _mock_query_tile(_z, _x, _y, _layer_type, _layer_name, _version):
    return FAKE_TILE


def _mock_query_tile_empty(_z, _x, _y, _layer_type, _layer_name, _version):
    return b""


class TestTilesRouterValidation:
    @patch("app.tiles.router._resolve_layer_version", _mock_resolve_version)
    @patch("app.tiles.router._query_tile", _mock_query_tile)
    def test_valid_layer_returns_200_with_mvt_content_type(self, client):
        response = client.get("/tiles/nn_catchments/10/507/338.mvt")
        assert response.status_code == 200
        assert response.headers["content-type"] == MVT_CONTENT_TYPE

    @patch("app.tiles.router._resolve_layer_version", _mock_resolve_version)
    @patch("app.tiles.router._query_tile", _mock_query_tile)
    def test_valid_layer_returns_tile_bytes(self, client):
        response = client.get("/tiles/lpa_boundaries/10/507/338.mvt")
        assert response.status_code == 200
        assert response.content == FAKE_TILE

    def test_unknown_layer_returns_404(self, client):
        response = client.get("/tiles/unknown_layer/10/507/338.mvt")
        assert response.status_code == 404
        assert response.json()["detail"] == "Unknown layer"

    @patch("app.tiles.router._resolve_layer_version", _mock_resolve_version)
    @patch("app.tiles.router._query_tile", _mock_query_tile)
    def test_zoom_above_max_returns_400(self, client):
        response = client.get("/tiles/nn_catchments/23/507/338.mvt")
        assert response.status_code == 400
        assert "Zoom level" in response.json()["detail"]

    @patch("app.tiles.router._resolve_layer_version", _mock_resolve_version)
    @patch("app.tiles.router._query_tile", _mock_query_tile)
    def test_zoom_below_min_returns_400(self, client):
        response = client.get("/tiles/nn_catchments/-1/507/338.mvt")
        assert response.status_code == 400

    @patch("app.tiles.router._resolve_layer_version", _mock_resolve_version)
    @patch("app.tiles.router._query_tile", _mock_query_tile_empty)
    def test_empty_tile_returns_200_with_empty_body(self, client):
        response = client.get("/tiles/nn_catchments/10/0/0.mvt")
        assert response.status_code == 200
        assert response.content == b""

    @patch("app.tiles.router._resolve_layer_version", _mock_resolve_version)
    @patch("app.tiles.router._query_tile", _mock_query_tile)
    def test_cache_control_header_present(self, client):
        response = client.get("/tiles/nn_catchments/10/507/338.mvt")
        assert response.status_code == 200
        assert response.headers.get("cache-control") == "public, max-age=3600"

    @patch("app.tiles.router._resolve_layer_version", _mock_resolve_version)
    @patch("app.tiles.router._query_tile", _mock_query_tile)
    def test_etag_header_present(self, client):
        response = client.get("/tiles/nn_catchments/10/507/338.mvt")
        assert response.status_code == 200
        assert response.headers.get("etag") is not None


class TestTilesRouterConditionalRequests:
    @patch("app.tiles.router._resolve_layer_version", _mock_resolve_version)
    @patch("app.tiles.router._query_tile", _mock_query_tile)
    def test_matching_etag_returns_304(self, client):
        response = client.get("/tiles/nn_catchments/10/507/338.mvt")
        etag = response.headers["etag"]

        response_304 = client.get(
            "/tiles/nn_catchments/10/507/338.mvt",
            headers={"If-None-Match": etag},
        )
        assert response_304.status_code == 304

    @patch("app.tiles.router._resolve_layer_version", _mock_resolve_version)
    @patch("app.tiles.router._query_tile", _mock_query_tile)
    def test_mismatched_etag_returns_200(self, client):
        response = client.get(
            "/tiles/nn_catchments/10/507/338.mvt",
            headers={"If-None-Match": '"stale-etag"'},
        )
        assert response.status_code == 200

    @patch("app.tiles.router._resolve_layer_version", _mock_resolve_version)
    @patch("app.tiles.router._query_tile", _mock_query_tile)
    def test_304_includes_etag_header(self, client):
        response = client.get("/tiles/nn_catchments/10/507/338.mvt")
        etag = response.headers["etag"]

        response_304 = client.get(
            "/tiles/nn_catchments/10/507/338.mvt",
            headers={"If-None-Match": etag},
        )
        assert response_304.headers.get("etag") == etag


class TestTilesRouterCache:
    @patch("app.tiles.router._resolve_layer_version", _mock_resolve_version)
    def test_cache_hit_on_second_request_calls_query_once(self, client):
        call_count = 0

        def counting_query_tile(z, x, y, layer_type, layer_name, version):
            nonlocal call_count
            call_count += 1
            return FAKE_TILE

        with patch("app.tiles.router._query_tile", counting_query_tile):
            client.get("/tiles/nn_catchments/10/507/338.mvt")
            client.get("/tiles/nn_catchments/10/507/338.mvt")

        assert call_count == 1

    @patch("app.tiles.router._resolve_layer_version", _mock_resolve_version)
    def test_different_coordinates_each_query_separately(self, client):
        call_count = 0

        def counting_query_tile(z, x, y, layer_type, layer_name, version):
            nonlocal call_count
            call_count += 1
            return FAKE_TILE

        with patch("app.tiles.router._query_tile", counting_query_tile):
            client.get("/tiles/nn_catchments/10/507/338.mvt")
            client.get("/tiles/nn_catchments/10/508/338.mvt")

        assert call_count == 2


class TestTilesRouterVersionResolution:
    def test_version_cache_populated_after_request(self, client):
        """After a tile request, _version_cache holds the resolved version."""
        from unittest.mock import MagicMock

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = (1,)
        mock_conn.__enter__ = lambda _: mock_conn
        mock_conn.__exit__ = MagicMock(return_value=False)

        mock_repo = MagicMock()
        mock_repo.engine.connect.return_value = mock_conn

        with (
            patch("app.tiles.router._get_repository", return_value=mock_repo),
            patch("app.tiles.router._query_tile", return_value=FAKE_TILE),
        ):
            response = client.get("/tiles/nn_catchments/10/507/338.mvt")

        assert response.status_code == 200
        cached = tiles_router_module._version_cache.get(
            tiles_router_module.TILE_LAYERS["nn_catchments"]
        )
        assert cached is not None
        resolved_version, _expiry = cached
        assert resolved_version == 1
