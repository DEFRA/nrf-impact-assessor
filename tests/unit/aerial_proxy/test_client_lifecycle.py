from unittest.mock import AsyncMock

import pytest


@pytest.mark.asyncio
async def test_close_client_closes_and_resets():
    """close_client() must close the httpx client and reset the singleton."""
    from app.aerial_proxy import router

    mock_client = AsyncMock()
    router._shared_client = mock_client

    await router.close_client()

    mock_client.aclose.assert_awaited_once()
    assert router._shared_client is None


@pytest.mark.asyncio
async def test_close_client_noop_when_no_client():
    """close_client() is safe to call when no client was ever created."""
    from app.aerial_proxy import router

    router._shared_client = None
    await router.close_client()  # should not raise
