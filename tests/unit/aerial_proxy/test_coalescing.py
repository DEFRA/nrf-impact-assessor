import asyncio
from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def _clear_module_state():
    """Reset module-level caches and inflight dict between tests."""
    from app.aerial_proxy import router

    router._cache_clear()
    router._inflight.clear()
    yield
    router._cache_clear()
    router._inflight.clear()


@pytest.mark.asyncio
async def test_cancelled_waiter_does_not_break_fetcher():
    """Cancelling a coalesced waiter must not crash the fetcher or other waiters."""
    from app.aerial_proxy.router import TileOutcome, TileResult, _get_tile

    fetch_started = asyncio.Event()
    fetch_proceed = asyncio.Event()

    async def slow_fetch(z, x, y):
        fetch_started.set()
        await fetch_proceed.wait()
        return TileResult(TileOutcome.HIT, b"tile-data", "image/jpeg")

    with (
        patch("app.aerial_proxy.router._fetch_upstream", side_effect=slow_fetch),
        patch("app.aerial_proxy.router._config") as mock_config,
    ):
        mock_config.base_url = "https://example.com"
        mock_config.cache_max_size = 100
        mock_config.cache_max_bytes = 128 * 1024 * 1024
        mock_config.cache_ttl_seconds = 3600

        fetcher_task = asyncio.create_task(_get_tile(1, 2, 3))
        await fetch_started.wait()

        waiter_task = asyncio.create_task(_get_tile(1, 2, 3))
        await asyncio.sleep(0.01)  # let waiter register

        waiter_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter_task

        fetch_proceed.set()
        result = await fetcher_task

    assert result == TileResult(TileOutcome.HIT, b"tile-data", "image/jpeg")


@pytest.mark.asyncio
async def test_surviving_waiters_get_result_after_one_cancels():
    """Other waiters still receive the tile when one waiter cancels."""
    from app.aerial_proxy.router import TileOutcome, TileResult, _get_tile

    fetch_started = asyncio.Event()
    fetch_proceed = asyncio.Event()

    async def slow_fetch(z, x, y):
        fetch_started.set()
        await fetch_proceed.wait()
        return TileResult(TileOutcome.HIT, b"tile-data", "image/jpeg")

    with (
        patch("app.aerial_proxy.router._fetch_upstream", side_effect=slow_fetch),
        patch("app.aerial_proxy.router._config") as mock_config,
    ):
        mock_config.base_url = "https://example.com"
        mock_config.cache_max_size = 100
        mock_config.cache_max_bytes = 128 * 1024 * 1024
        mock_config.cache_ttl_seconds = 3600

        t1 = asyncio.create_task(_get_tile(1, 2, 3))
        await fetch_started.wait()

        t2 = asyncio.create_task(_get_tile(1, 2, 3))
        t3 = asyncio.create_task(_get_tile(1, 2, 3))
        await asyncio.sleep(0.01)

        t2.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t2

        fetch_proceed.set()
        r1 = await t1
        r3 = await t3

    expected = TileResult(TileOutcome.HIT, b"tile-data", "image/jpeg")
    assert r1 == expected
    assert r3 == expected
