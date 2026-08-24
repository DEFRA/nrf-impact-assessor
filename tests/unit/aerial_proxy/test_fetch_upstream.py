from unittest.mock import AsyncMock, patch

import httpx
import pytest

from app.aerial_proxy.router import _MISSING, _UPSTREAM_ERROR, TileOutcome, TileResult


@pytest.fixture(autouse=True)
def _clear_module_state():
    from app.aerial_proxy import router

    router._cache_clear()
    router._inflight.clear()
    yield
    router._cache_clear()
    router._inflight.clear()


class _FakeStreamResponse:
    """Minimal stand-in for the streaming httpx.Response context manager."""

    def __init__(self, status_code=200, chunks=(b"image-data",), content_type=None):
        self.status_code = status_code
        self._chunks = list(chunks)
        self.headers = {}
        if content_type is not None:
            self.headers["content-type"] = content_type

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False

    async def aiter_bytes(self, chunk_size=None):
        for chunk in self._chunks:
            yield chunk


def _mock_client(response=None, stream_error=None):
    client = AsyncMock()

    def stream(*_args, **_kwargs):
        if stream_error is not None:
            raise stream_error
        return response

    client.stream = stream
    return client


async def _fetch(client, **config_overrides):
    from app.aerial_proxy.router import _fetch_upstream

    with (
        patch("app.aerial_proxy.router._get_client", return_value=client),
        patch("app.aerial_proxy.router._config") as cfg,
    ):
        cfg.base_url = "https://example.com/wmts?SERVICE=WMTS&LAYER=APGB"
        cfg.max_tile_bytes = 8 * 1024 * 1024
        for key, value in config_overrides.items():
            setattr(cfg, key, value)
        return await _fetch_upstream(11, 1030, 674)


@pytest.mark.asyncio
async def test_valid_jpeg_tile_returned():
    """A normal 200 image/jpeg response is returned."""
    resp = _FakeStreamResponse(chunks=[b"\xff\xd8tile"], content_type="image/jpeg")
    result = await _fetch(_mock_client(resp))

    assert result == TileResult(TileOutcome.HIT, b"\xff\xd8tile", "image/jpeg")


@pytest.mark.asyncio
async def test_html_200_not_returned_as_tile():
    """A 200 with text/html content type must be rejected."""
    resp = _FakeStreamResponse(chunks=[b"<html>Error</html>"], content_type="text/html")

    assert await _fetch(_mock_client(resp)) == _UPSTREAM_ERROR


@pytest.mark.asyncio
async def test_svg_content_type_rejected():
    """An SVG response (image/svg+xml) must be rejected as unsupported."""
    resp = _FakeStreamResponse(chunks=[b"<svg></svg>"], content_type="image/svg+xml")

    assert await _fetch(_mock_client(resp)) == _UPSTREAM_ERROR


@pytest.mark.asyncio
async def test_3xx_with_image_content_type_rejected():
    """A non-2xx status with image content-type must not be cached as a tile."""
    resp = _FakeStreamResponse(
        status_code=302, chunks=[b"\xff\xd8fake"], content_type="image/jpeg"
    )

    assert await _fetch(_mock_client(resp)) == _UPSTREAM_ERROR


@pytest.mark.asyncio
async def test_content_type_with_charset_accepted():
    """image/png with charset parameter must still be accepted."""
    resp = _FakeStreamResponse(
        chunks=[b"\x89PNGtile"], content_type="image/png; charset=utf-8"
    )
    result = await _fetch(_mock_client(resp))

    assert result.outcome is TileOutcome.HIT
    assert result.content_type == "image/png; charset=utf-8"


@pytest.mark.asyncio
async def test_404_reports_missing():
    resp = _FakeStreamResponse(status_code=404, chunks=[], content_type="text/plain")

    assert await _fetch(_mock_client(resp)) == _MISSING


@pytest.mark.asyncio
async def test_empty_body_reports_missing():
    resp = _FakeStreamResponse(chunks=[], content_type="image/jpeg")

    assert await _fetch(_mock_client(resp)) == _MISSING


@pytest.mark.asyncio
async def test_follow_redirects_is_passed():
    resp = _FakeStreamResponse(chunks=[b"\xff\xd8tile"], content_type="image/jpeg")
    client = AsyncMock()
    calls = []

    def stream(*args, **kwargs):
        calls.append((args, kwargs))
        return resp

    client.stream = stream
    result = await _fetch(client)

    assert result.outcome is TileOutcome.HIT
    assert calls[0][1].get("follow_redirects") is True


# ---------------------------------------------------------------------------
# Response size cap
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_oversized_body_rejected_while_streaming():
    """A body exceeding max_tile_bytes must be abandoned, not buffered."""
    resp = _FakeStreamResponse(chunks=[b"x" * 6, b"x" * 6], content_type="image/jpeg")

    assert await _fetch(_mock_client(resp), max_tile_bytes=10) == _UPSTREAM_ERROR


@pytest.mark.asyncio
async def test_body_at_limit_accepted():
    resp = _FakeStreamResponse(chunks=[b"x" * 10], content_type="image/jpeg")

    assert await _fetch(_mock_client(resp), max_tile_bytes=10) == TileResult(
        TileOutcome.HIT, b"x" * 10, "image/jpeg"
    )


@pytest.mark.asyncio
async def test_oversized_content_length_rejected_without_reading():
    """An oversized declared content-length short-circuits before the body."""
    resp = _FakeStreamResponse(chunks=[b"x"], content_type="image/jpeg")
    resp.headers["content-length"] = "999999"

    assert await _fetch(_mock_client(resp), max_tile_bytes=10) == _UPSTREAM_ERROR


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_timeout_reports_upstream_error():
    client = _mock_client(stream_error=httpx.ConnectTimeout("timed out"))

    assert await _fetch(client) == _UPSTREAM_ERROR


@pytest.mark.asyncio
async def test_invalid_url_reports_upstream_error_not_500():
    """httpx.InvalidURL is not an HTTPError; it must not escape as a 500."""
    client = _mock_client(stream_error=httpx.InvalidURL("no scheme"))

    assert await _fetch(client) == _UPSTREAM_ERROR


@pytest.mark.asyncio
async def test_scheme_less_base_url_reports_upstream_error():
    """A misconfigured base_url must degrade to the placeholder, not crash."""
    from app.aerial_proxy.router import _fetch_upstream

    async with httpx.AsyncClient() as real_client:
        with (
            patch("app.aerial_proxy.router._get_client", return_value=real_client),
            patch("app.aerial_proxy.router._config") as cfg,
        ):
            cfg.base_url = "tiles.example.com"
            cfg.max_tile_bytes = 8 * 1024 * 1024
            assert await _fetch_upstream(11, 1030, 674) == _UPSTREAM_ERROR


# ---------------------------------------------------------------------------
# Upstream URL construction
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "base_url",
    [
        (
            "https://example.com/APGB.wmtsx?SERVICE=WMTS&REQUEST=GetTile&VERSION=1.0.0"
            "&LAYER=APGB_Latest_UK_250mm&STYLE=Default&FORMAT=image%2Fpng"
            "&TILEMATRIXSET=GoogleMapsExtended"
        ),
        # Stale tile coordinates in the configured URL are replaced, not appended.
        (
            "https://example.com/APGB.wmtsx?SERVICE=WMTS&REQUEST=GetTile&VERSION=1.0.0"
            "&LAYER=APGB_Latest_UK_250mm&STYLE=Default&FORMAT=image%2Fpng"
            "&TILEMATRIXSET=GoogleMapsExtended&TILEMATRIX=1&TILEROW=2&TILECOL=3"
        ),
    ],
)
def test_build_upstream_url_sets_tile_coordinates(base_url):
    from urllib.parse import parse_qs, urlsplit

    from app.aerial_proxy.router import _build_upstream_url

    with patch("app.aerial_proxy.router._config") as cfg:
        cfg.base_url = base_url
        url = _build_upstream_url(11, 1030, 674)

    parsed = urlsplit(url)
    params = parse_qs(parsed.query)

    assert parsed.netloc == "example.com"
    assert parsed.path == "/APGB.wmtsx"
    assert params["TILEMATRIX"] == ["11"]
    assert params["TILECOL"] == ["1030"]
    assert params["TILEROW"] == ["674"]
    # Non-tile parameters survive untouched, including encoded values.
    assert params["LAYER"] == ["APGB_Latest_UK_250mm"]
    assert params["FORMAT"] == ["image/png"]
    assert params["TILEMATRIXSET"] == ["GoogleMapsExtended"]


@pytest.mark.asyncio
async def test_absent_content_type_rejected():
    """A response with no content-type must not be assumed to be imagery."""
    resp = _FakeStreamResponse(chunks=[b"<html>Error</html>"], content_type=None)

    assert await _fetch(_mock_client(resp)) == _UPSTREAM_ERROR


@pytest.mark.asyncio
async def test_blank_content_type_rejected():
    resp = _FakeStreamResponse(chunks=[b"\xff\xd8tile"], content_type="   ")

    assert await _fetch(_mock_client(resp)) == _UPSTREAM_ERROR
