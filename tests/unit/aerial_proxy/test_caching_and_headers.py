"""Negative caching and the cache/outcome response headers."""

from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.aerial_proxy.router import (
    _MISSING,
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


def _config_patch():
    cfg = patch("app.aerial_proxy.router._config").start()
    cfg.base_url = "https://example.com/wmts?SERVICE=WMTS&LAYER=APGB"
    cfg.cache_max_size = 100
    cfg.cache_max_bytes = 128 * 1024 * 1024
    cfg.cache_ttl_seconds = 3600
    cfg.missing_cache_ttl_seconds = 300
    cfg.error_cache_ttl_seconds = 15
    cfg.max_tile_bytes = 2 * 1024 * 1024
    return cfg


@pytest.fixture
def config():
    cfg = _config_patch()
    yield cfg
    patch.stopall()


_HIT = TileResult(TileOutcome.HIT, b"\xff\xd8tile", "image/jpeg")


# ---------------------------------------------------------------------------
# Negative caching
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("result", [_HIT, _MISSING, _UPSTREAM_ERROR])
@pytest.mark.asyncio
async def test_every_outcome_is_cached(config, result):
    """Misses and errors are cached too, so a second request skips upstream."""
    from app.aerial_proxy.router import _get_tile

    with patch("app.aerial_proxy.router._fetch_upstream", return_value=result) as fetch:
        assert await _get_tile(11, 1030, 674) == result
        assert await _get_tile(11, 1030, 674) == result

    assert fetch.call_count == 1


@pytest.mark.parametrize(
    ("result", "expected_ttl"),
    [(_HIT, 3600), (_MISSING, 300), (_UPSTREAM_ERROR, 15)],
)
def test_ttl_per_outcome(config, result, expected_ttl):
    """Errors expire far sooner than hits, so an outage recovers quickly."""
    assert result.ttl_seconds == expected_ttl


@pytest.mark.asyncio
async def test_expired_negative_entry_is_refetched(config):
    """Once the short error TTL lapses the upstream is tried again."""
    from app.aerial_proxy.router import _get_tile

    with patch(
        "app.aerial_proxy.router._fetch_upstream", return_value=_UPSTREAM_ERROR
    ) as fetch:
        assert await _get_tile(11, 1030, 674) == _UPSTREAM_ERROR

        # Advance past the error TTL without waiting for it.
        with patch("app.aerial_proxy.router.time.monotonic", return_value=1e9):
            assert await _get_tile(11, 1030, 674) == _UPSTREAM_ERROR

    assert fetch.call_count == 2


# ---------------------------------------------------------------------------
# Response headers
# ---------------------------------------------------------------------------


def test_hit_headers_derive_max_age_from_config(config, client):
    """Cache-Control must follow cache_ttl_seconds, not a hardcoded hour."""
    config.cache_ttl_seconds = 42

    with patch("app.aerial_proxy.router._fetch_upstream", return_value=_HIT):
        resp = client.get("/aerial_proxy/11/1030/674")

    assert resp.status_code == 200
    assert resp.content == b"\xff\xd8tile"
    assert resp.headers["content-type"] == "image/jpeg"
    assert resp.headers["cache-control"] == "private, max-age=42"
    assert resp.headers["x-aerial-proxy-tile"] == "hit"


@pytest.mark.parametrize(
    ("result", "expected_outcome", "expected_max_age"),
    [(_MISSING, "missing", 300), (_UPSTREAM_ERROR, "upstream-error", 15)],
)
def test_placeholder_headers_distinguish_outcomes(
    config, client, result, expected_outcome, expected_max_age
):
    """Both failure modes serve the placeholder but are distinguishable."""
    from app.aerial_proxy.router import _NOT_AVAILABLE_TILE

    with patch("app.aerial_proxy.router._fetch_upstream", return_value=result):
        resp = client.get("/aerial_proxy/11/1030/674")

    assert resp.status_code == 200
    assert resp.content == _NOT_AVAILABLE_TILE
    assert resp.headers["content-type"] == "image/png"
    assert resp.headers["x-aerial-proxy-tile"] == expected_outcome
    assert resp.headers["cache-control"] == f"private, max-age={expected_max_age}"


def test_unconfigured_base_url_returns_502(config, client):
    config.base_url = ""

    resp = client.get("/aerial_proxy/11/1030/674")

    assert resp.status_code == 502


# ---------------------------------------------------------------------------
# Cache byte budget
# ---------------------------------------------------------------------------


def _tile(size: int) -> TileResult:
    return TileResult(TileOutcome.HIT, b"x" * size, "image/jpeg")


def test_cache_evicts_on_byte_budget_before_entry_count(config):
    """The byte budget bounds memory even with entry count far from its cap."""
    from app.aerial_proxy import router as router_module

    config.cache_max_size = 100
    config.cache_max_bytes = 1000

    for i in range(20):
        router_module._cache_put((0, 0, i), _tile(300))

    assert router_module._tile_cache_bytes <= 1000
    assert len(router_module._tile_cache) == 3


def test_byte_total_tracks_expiry_and_replacement(config):
    """Evictions, expiry and re-puts must all keep the byte total in step."""
    from app.aerial_proxy import router as router_module

    config.cache_max_bytes = 10_000

    router_module._cache_put((1, 1, 1), _tile(400))
    assert router_module._tile_cache_bytes == 400

    # Replacing the same key must not double-count.
    router_module._cache_put((1, 1, 1), _tile(100))
    assert router_module._tile_cache_bytes == 100

    # An expired entry dropped by _cache_get must release its bytes.
    with patch("app.aerial_proxy.router.time.monotonic", return_value=1e9):
        assert router_module._cache_get((1, 1, 1)) is None
    assert router_module._tile_cache_bytes == 0


def test_tile_larger_than_budget_is_not_cached(config):
    """An oversized payload must not evict the whole cache to store itself."""
    from app.aerial_proxy import router as router_module

    config.cache_max_bytes = 1000

    router_module._cache_put((1, 1, 1), _tile(500))
    router_module._cache_put((2, 2, 2), _tile(2000))

    assert list(router_module._tile_cache) == [(1, 1, 1)]
    assert router_module._tile_cache_bytes == 500


def test_negative_entries_cost_no_bytes(config):
    """Negative caching must not consume the byte budget."""
    from app.aerial_proxy import router as router_module

    router_module._cache_put((1, 1, 1), _MISSING)
    router_module._cache_put((2, 2, 2), _UPSTREAM_ERROR)

    assert router_module._tile_cache_bytes == 0
    assert len(router_module._tile_cache) == 2
