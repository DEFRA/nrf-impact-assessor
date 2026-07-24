"""Tests for GET /tiles/{layer}/{z}/{x}/{y}.mvt"""

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

import app.tiles.router as tiles_router_module
from app.main import app
from app.tiles.router import TILE_LAYERS

MVT_CONTENT_TYPE = "application/vnd.mapbox-vector-tile"
FAKE_TILE = b"\x1a\x00"  # minimal non-empty bytes


@pytest.fixture(autouse=True)
def reset_tile_caches():
    """Clear module-level caches before each test for isolation."""
    tiles_router_module._tile_cache.clear()
    tiles_router_module._version_cache.clear()
    yield
    tiles_router_module._tile_cache.clear()
    tiles_router_module._version_cache.clear()


@pytest.fixture
def client():
    return TestClient(app)


def _mock_resolve_version(_slug):
    return 1


def _mock_query_tile(_z, _x, _y, _slug, _layer_name, _version, _timings):
    return FAKE_TILE


def _mock_query_tile_empty(_z, _x, _y, _slug, _layer_name, _version, _timings):
    return b""


class TestTilesRouterLayers:
    """Which layers the endpoint serves."""

    def test_only_the_two_edp_layers_are_whitelisted(self):
        assert TILE_LAYERS == {
            "edp_boundaries": "public.edp_boundary_layer",
            "edp_excluded_areas": "public.edp_excluded_areas",
        }

    @pytest.mark.parametrize("slug", sorted(TILE_LAYERS))
    @patch("app.tiles.router._resolve_layer_version", _mock_resolve_version)
    @patch("app.tiles.router._query_tile", _mock_query_tile)
    def test_each_whitelisted_layer_is_served(self, client, slug):
        response = client.get(f"/tiles/{slug}/10/507/338.mvt")
        assert response.status_code == 200
        assert response.content == FAKE_TILE

    def test_each_slug_maps_to_a_distinct_table(self):
        """Guards the bug this whitelist replaced: a second EDP slug used to
        reach a query with edp_boundary_layer hardcoded, so it returned the
        boundary geometry under the other layer's name."""
        tables = list(TILE_LAYERS.values())
        assert len(tables) == len(set(tables))


class TestTilesRouterSqlTargeting:
    """Each slug must query its own table and label the MVT with its own name."""

    @pytest.mark.parametrize("slug", sorted(TILE_LAYERS))
    def test_query_targets_the_mapped_table(self, client, slug):
        captured = {}

        mock_conn = MagicMock()

        def capture_execute(sql, params=None):
            # The version lookup passes no params; the tile query passes them.
            if params is not None:
                captured["sql"] = str(sql)
                captured["params"] = params
            result = MagicMock()
            result.fetchone.return_value = (1,) if params is None else (FAKE_TILE,)
            return result

        mock_conn.execute.side_effect = capture_execute
        mock_conn.__enter__ = lambda _: mock_conn
        mock_conn.__exit__ = MagicMock(return_value=False)

        mock_repo = MagicMock()
        mock_repo.engine.connect.return_value = mock_conn

        with patch("app.tiles.router._get_repository", return_value=mock_repo):
            response = client.get(f"/tiles/{slug}/10/507/338.mvt")

        assert response.status_code == 200
        assert TILE_LAYERS[slug] in captured["sql"]
        # The MVT layer label is the slug — map clients key `source-layer` on it.
        assert captured["params"]["layer_name"] == slug

        # No other layer's table leaked into this query.
        for other_slug, other_table in TILE_LAYERS.items():
            if other_slug != slug:
                assert other_table not in captured["sql"]


class TestTilesRouterValidation:
    @patch("app.tiles.router._resolve_layer_version", _mock_resolve_version)
    @patch("app.tiles.router._query_tile", _mock_query_tile)
    def test_valid_layer_returns_200_with_mvt_content_type(self, client):
        response = client.get("/tiles/edp_excluded_areas/10/507/338.mvt")
        assert response.status_code == 200
        assert response.headers["content-type"] == MVT_CONTENT_TYPE

    def test_unknown_layer_returns_404(self, client):
        response = client.get("/tiles/unknown_layer/10/507/338.mvt")
        assert response.status_code == 404
        assert response.json()["detail"] == "Unknown layer"

    @patch("app.tiles.router._resolve_layer_version", _mock_resolve_version)
    @patch("app.tiles.router._query_tile", _mock_query_tile)
    def test_zoom_above_max_returns_400(self, client):
        response = client.get("/tiles/edp_excluded_areas/23/507/338.mvt")
        assert response.status_code == 400
        assert "Zoom level" in response.json()["detail"]

    @patch("app.tiles.router._resolve_layer_version", _mock_resolve_version)
    @patch("app.tiles.router._query_tile", _mock_query_tile)
    def test_zoom_below_min_returns_400(self, client):
        response = client.get("/tiles/edp_excluded_areas/-1/507/338.mvt")
        assert response.status_code == 400

    @patch("app.tiles.router._resolve_layer_version", _mock_resolve_version)
    @patch("app.tiles.router._query_tile", _mock_query_tile_empty)
    def test_empty_tile_returns_200_with_empty_body(self, client):
        response = client.get("/tiles/edp_excluded_areas/10/0/0.mvt")
        assert response.status_code == 200
        assert response.content == b""

    @patch("app.tiles.router._resolve_layer_version", _mock_resolve_version)
    @patch("app.tiles.router._query_tile", _mock_query_tile)
    def test_cache_control_header_present(self, client):
        response = client.get("/tiles/edp_excluded_areas/10/507/338.mvt")
        assert response.status_code == 200
        assert response.headers.get("cache-control") == "public, max-age=3600"

    @patch("app.tiles.router._resolve_layer_version", _mock_resolve_version)
    @patch("app.tiles.router._query_tile", _mock_query_tile)
    def test_etag_header_present(self, client):
        response = client.get("/tiles/edp_excluded_areas/10/507/338.mvt")
        assert response.status_code == 200
        assert response.headers.get("etag") is not None


class TestTilesRouterConditionalRequests:
    @patch("app.tiles.router._resolve_layer_version", _mock_resolve_version)
    @patch("app.tiles.router._query_tile", _mock_query_tile)
    def test_matching_etag_returns_304(self, client):
        response = client.get("/tiles/edp_excluded_areas/10/507/338.mvt")
        etag = response.headers["etag"]

        response_304 = client.get(
            "/tiles/edp_excluded_areas/10/507/338.mvt",
            headers={"If-None-Match": etag},
        )
        assert response_304.status_code == 304

    @patch("app.tiles.router._resolve_layer_version", _mock_resolve_version)
    @patch("app.tiles.router._query_tile", _mock_query_tile)
    def test_mismatched_etag_returns_200(self, client):
        response = client.get(
            "/tiles/edp_excluded_areas/10/507/338.mvt",
            headers={"If-None-Match": '"stale-etag"'},
        )
        assert response.status_code == 200

    @patch("app.tiles.router._resolve_layer_version", _mock_resolve_version)
    @patch("app.tiles.router._query_tile", _mock_query_tile)
    def test_304_includes_etag_header(self, client):
        response = client.get("/tiles/edp_excluded_areas/10/507/338.mvt")
        etag = response.headers["etag"]

        response_304 = client.get(
            "/tiles/edp_excluded_areas/10/507/338.mvt",
            headers={"If-None-Match": etag},
        )
        assert response_304.headers.get("etag") == etag

    @patch("app.tiles.router._resolve_layer_version", _mock_resolve_version)
    @patch("app.tiles.router._query_tile", _mock_query_tile)
    def test_etag_differs_between_layers_for_the_same_tile(self, client):
        """Same z/x/y on different layers must not collide in an HTTP cache."""
        boundaries = client.get("/tiles/edp_boundaries/10/507/338.mvt")
        excluded = client.get("/tiles/edp_excluded_areas/10/507/338.mvt")
        assert boundaries.headers["etag"] != excluded.headers["etag"]


class TestTilesRouterCache:
    @patch("app.tiles.router._resolve_layer_version", _mock_resolve_version)
    def test_cache_hit_on_second_request_calls_query_once(self, client):
        call_count = 0

        def counting_query_tile(z, x, y, slug, layer_name, version, timings):
            nonlocal call_count
            call_count += 1
            return FAKE_TILE

        with patch("app.tiles.router._query_tile", counting_query_tile):
            client.get("/tiles/edp_excluded_areas/10/507/338.mvt")
            client.get("/tiles/edp_excluded_areas/10/507/338.mvt")

        assert call_count == 1

    @patch("app.tiles.router._resolve_layer_version", _mock_resolve_version)
    def test_different_coordinates_each_query_separately(self, client):
        call_count = 0

        def counting_query_tile(z, x, y, slug, layer_name, version, timings):
            nonlocal call_count
            call_count += 1
            return FAKE_TILE

        with patch("app.tiles.router._query_tile", counting_query_tile):
            client.get("/tiles/edp_excluded_areas/10/507/338.mvt")
            client.get("/tiles/edp_excluded_areas/10/508/338.mvt")

        assert call_count == 2

    @patch("app.tiles.router._resolve_layer_version", _mock_resolve_version)
    def test_layers_do_not_share_cache_entries(self, client):
        """The cache key includes the slug, so one layer cannot serve another's
        bytes for the same tile coordinates."""
        seen_slugs = []

        def recording_query_tile(z, x, y, slug, layer_name, version, timings):
            seen_slugs.append(slug)
            return FAKE_TILE

        with patch("app.tiles.router._query_tile", recording_query_tile):
            client.get("/tiles/edp_boundaries/10/507/338.mvt")
            client.get("/tiles/edp_excluded_areas/10/507/338.mvt")

        assert seen_slugs == ["edp_boundaries", "edp_excluded_areas"]


class TestTilesRouterVersionResolution:
    @pytest.mark.parametrize("slug", sorted(TILE_LAYERS))
    def test_version_cache_populated_after_request(self, client, slug):
        """After a tile request, _version_cache holds the resolved version.

        Both EDP layers now go through the shared per-slug cache; there is no
        longer a separate single-table cache for the boundary layer.
        """
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
            response = client.get(f"/tiles/{slug}/10/507/338.mvt")

        assert response.status_code == 200
        cached = tiles_router_module._version_cache.get(slug)
        assert cached is not None
        resolved_version, _expiry = cached
        assert resolved_version == 1
