"""ETag generation and If-None-Match revalidation."""

import hashlib
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.aerial_proxy.router import (
    _MISSING,
    _NOT_AVAILABLE_TILE,
    _UPSTREAM_ERROR,
    TileOutcome,
    TileResult,
    router,
)


@pytest.fixture(autouse=True)
def _clear_module_state():
    from app.aerial_proxy import router as router_module

    router_module._cache_clear()
    router_module._inflight.clear()
    yield
    router_module._cache_clear()
    router_module._inflight.clear()


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(router)
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def config():
    cfg = patch("app.aerial_proxy.router._config").start()
    cfg.base_url = "https://example.com/wmts?SERVICE=WMTS&LAYER=APGB"
    cfg.cache_max_size = 100
    cfg.cache_max_bytes = 128 * 1024 * 1024
    cfg.cache_ttl_seconds = 3600
    cfg.missing_cache_ttl_seconds = 300
    cfg.error_cache_ttl_seconds = 15
    cfg.max_tile_bytes = 2 * 1024 * 1024
    yield cfg
    patch.stopall()


_HIT = TileResult(TileOutcome.HIT, b"\xff\xd8tile", "image/jpeg")
_HIT_ETAG = f'"{hashlib.sha256(b"\xff\xd8tile").hexdigest()}"'
_PLACEHOLDER_ETAG = f'"{hashlib.sha256(_NOT_AVAILABLE_TILE).hexdigest()}"'


# ---------------------------------------------------------------------------
# ETag derivation
# ---------------------------------------------------------------------------


def test_hit_etag_is_strong_validator_over_tile_bytes(config, client):
    with patch("app.aerial_proxy.router._fetch_upstream", return_value=_HIT):
        resp = client.get("/aerial_proxy/11/1030/674")

    assert resp.status_code == 200
    assert resp.headers["etag"] == _HIT_ETAG


def test_different_bytes_yield_different_etags():
    """The validator must actually track content, not just the coordinates."""
    one = TileResult(TileOutcome.HIT, b"aaa", "image/png")
    two = TileResult(TileOutcome.HIT, b"bbb", "image/png")

    assert one.etag != two.etag
    assert TileResult(TileOutcome.HIT, b"aaa", "image/png").etag == one.etag


@pytest.mark.parametrize("result", [_MISSING, _UPSTREAM_ERROR])
def test_placeholder_outcomes_share_the_placeholder_etag(config, client, result):
    """Both failure modes send the same bytes, so they share one validator."""
    with patch("app.aerial_proxy.router._fetch_upstream", return_value=result):
        resp = client.get("/aerial_proxy/11/1030/674")

    assert resp.status_code == 200
    assert resp.headers["etag"] == _PLACEHOLDER_ETAG


def test_placeholder_and_hit_etags_differ(config, client):
    """A tile that gains imagery must not revalidate as unchanged."""
    assert _MISSING.etag != _HIT.etag


# ---------------------------------------------------------------------------
# If-None-Match revalidation
# ---------------------------------------------------------------------------


def test_matching_if_none_match_returns_304_with_no_body(config, client):
    with patch("app.aerial_proxy.router._fetch_upstream", return_value=_HIT):
        resp = client.get(
            "/aerial_proxy/11/1030/674", headers={"If-None-Match": _HIT_ETAG}
        )

    assert resp.status_code == 304
    assert resp.content == b""


def test_304_still_carries_the_refreshed_cache_headers(config, client):
    """A revalidation must extend freshness, so it repeats the directives."""
    config.cache_ttl_seconds = 42

    with patch("app.aerial_proxy.router._fetch_upstream", return_value=_HIT):
        resp = client.get(
            "/aerial_proxy/11/1030/674", headers={"If-None-Match": _HIT_ETAG}
        )

    assert resp.status_code == 304
    assert resp.headers["etag"] == _HIT_ETAG
    assert resp.headers["cache-control"] == "private, max-age=42"
    assert resp.headers["x-aerial-proxy-tile"] == "hit"


def test_non_matching_if_none_match_returns_the_tile(config, client):
    with patch("app.aerial_proxy.router._fetch_upstream", return_value=_HIT):
        resp = client.get(
            "/aerial_proxy/11/1030/674", headers={"If-None-Match": '"stale"'}
        )

    assert resp.status_code == 200
    assert resp.content == b"\xff\xd8tile"


def test_stale_placeholder_etag_does_not_match_a_real_tile(config, client):
    """The client cached a placeholder; imagery now exists, so send it."""
    with patch("app.aerial_proxy.router._fetch_upstream", return_value=_HIT):
        resp = client.get(
            "/aerial_proxy/11/1030/674", headers={"If-None-Match": _PLACEHOLDER_ETAG}
        )

    assert resp.status_code == 200
    assert resp.content == b"\xff\xd8tile"


def test_weak_validator_matches_under_weak_comparison(config, client):
    """RFC 9110 requires If-None-Match to use weak comparison."""
    with patch("app.aerial_proxy.router._fetch_upstream", return_value=_HIT):
        resp = client.get(
            "/aerial_proxy/11/1030/674", headers={"If-None-Match": f"W/{_HIT_ETAG}"}
        )

    assert resp.status_code == 304


def test_wildcard_if_none_match_matches_any_representation(config, client):
    with patch("app.aerial_proxy.router._fetch_upstream", return_value=_HIT):
        resp = client.get("/aerial_proxy/11/1030/674", headers={"If-None-Match": "*"})

    assert resp.status_code == 304


def test_etag_list_matches_on_any_member(config, client):
    with patch("app.aerial_proxy.router._fetch_upstream", return_value=_HIT):
        resp = client.get(
            "/aerial_proxy/11/1030/674",
            headers={"If-None-Match": f'"other", W/"another", {_HIT_ETAG}'},
        )

    assert resp.status_code == 304


@pytest.mark.parametrize("result", [_MISSING, _UPSTREAM_ERROR])
def test_placeholder_revalidates_to_304(config, client, result):
    """A client holding the placeholder need not re-download it every TTL."""
    with patch("app.aerial_proxy.router._fetch_upstream", return_value=result):
        resp = client.get(
            "/aerial_proxy/11/1030/674", headers={"If-None-Match": _PLACEHOLDER_ETAG}
        )

    assert resp.status_code == 304


def test_unconfigured_base_url_still_502s_despite_if_none_match(config, client):
    """Revalidation must not short-circuit the configuration check."""
    config.base_url = ""

    resp = client.get("/aerial_proxy/11/1030/674", headers={"If-None-Match": "*"})

    assert resp.status_code == 502
