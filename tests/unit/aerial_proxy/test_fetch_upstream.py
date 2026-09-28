import io
from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs, urlsplit

import httpx
import numpy as np
import pytest
from PIL import Image, ImageDraw
from pyproj import Transformer

from app.aerial_proxy.router import _MISSING, _UPSTREAM_ERROR, TileOutcome, TileResult
from app.config import AERIAL_WMS_MIN_ZOOM


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
        cfg.wms_base_url = ""
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
            cfg.wms_base_url = ""
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
    from app.aerial_proxy.router import _build_upstream_url

    with patch("app.aerial_proxy.router._config") as cfg:
        cfg.base_url = base_url
        cfg.wms_base_url = ""
        url, window = _build_upstream_url(11, 1030, 674)

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
    # WMTS tiles are served as they come, with no reprojection.
    assert window is None


@pytest.mark.asyncio
async def test_absent_content_type_rejected():
    """A response with no content-type must not be assumed to be imagery."""
    resp = _FakeStreamResponse(chunks=[b"<html>Error</html>"], content_type=None)

    assert await _fetch(_mock_client(resp)) == _UPSTREAM_ERROR


@pytest.mark.asyncio
async def test_blank_content_type_rejected():
    resp = _FakeStreamResponse(chunks=[b"\xff\xd8tile"], content_type="   ")

    assert await _fetch(_mock_client(resp)) == _UPSTREAM_ERROR


# ---------------------------------------------------------------------------
# WMTS / WMS handover
#
# The WMTS pyramid serves the low zooms, where its fixed grid caches well;
# past the handover zoom the WMS renders tiles on demand instead.  The WMS is
# asked for British National Grid imagery, which the proxy reprojects to Web
# Mercator itself with OSTN15: the upstream's own Web Mercator rendering sits
# metres off the OS basemap.
# ---------------------------------------------------------------------------

_WMTS_BASE = (
    "https://example.com/APGB.wmtsx?SERVICE=WMTS&REQUEST=GetTile&VERSION=1.0.0"
    "&LAYER=APGB_Latest_UK_125mm&STYLE=Default&FORMAT=image%2Fjpeg"
    "&TILEMATRIXSET=GoogleMapsExtended"
)
# Configured with the Web Mercator CRS the proxy used to request.
_WMS_BASE = (
    "https://example.com/Apgb.wmsx?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetMap"
    "&LAYERS=APGB_Latest_UK_125mm&STYLES=&CRS=EPSG%3A3857&FORMAT=image%2Fjpeg"
)

_WEB_MERCATOR_ORIGIN = 20037508.342789244
_TILE_PIXELS = 256
_WESTMINSTER = (19, 261962, 174354)
_TO_BNG = Transformer.from_crs(3857, 27700, always_xy=True)


def _web_mercator_bounds(z, x, y):
    span = 2 * _WEB_MERCATOR_ORIGIN / 2**z
    left = -_WEB_MERCATOR_ORIGIN + x * span
    top = _WEB_MERCATOR_ORIGIN - y * span
    return left, top - span, left + span, top


def _bbox(params):
    return tuple(float(v) for v in params["BBOX"][0].split(","))


def _upstream_query(z, x, y, *, wms_base_url=_WMS_BASE, wms_min_zoom=0):
    """Build the upstream URL for one tile and return its parsed query.

    `wms_min_zoom` defaults to 0 so tests about WMS request shape do not
    pin the handover; the tests about the handover set it explicitly.
    """
    from app.aerial_proxy.router import _build_upstream_url

    with (
        patch("app.aerial_proxy.router._config") as cfg,
        patch("app.aerial_proxy.router.AERIAL_WMS_MIN_ZOOM", wms_min_zoom),
    ):
        cfg.base_url = _WMTS_BASE
        cfg.wms_base_url = wms_base_url
        url, _window = _build_upstream_url(z, x, y)

    parsed = urlsplit(url)
    return parsed, parse_qs(parsed.query)


def test_below_handover_zoom_uses_wmts():
    z = AERIAL_WMS_MIN_ZOOM - 1
    parsed, params = _upstream_query(z, 4093, 2724, wms_min_zoom=AERIAL_WMS_MIN_ZOOM)

    assert parsed.path == "/APGB.wmtsx"
    assert params["TILEMATRIX"] == [str(z)]
    assert params["TILECOL"] == ["4093"]
    assert params["TILEROW"] == ["2724"]
    assert "BBOX" not in params


def test_at_and_above_handover_zoom_uses_wms_on_the_national_grid():
    for z in (AERIAL_WMS_MIN_ZOOM, AERIAL_WMS_MIN_ZOOM + 1, 21):
        parsed, params = _upstream_query(
            z, 2**z // 2, 2**z // 3, wms_min_zoom=AERIAL_WMS_MIN_ZOOM
        )

        assert parsed.path == "/Apgb.wmsx", f"z{z} should route to the WMS"
        # The configured CRS is overridden, never sent twice.
        assert params["CRS"] == ["EPSG:27700"]
        assert "BBOX" in params
        # WMTS coordinates are meaningless to the WMS and must not leak.
        assert "TILEMATRIX" not in params
        assert "TILEROW" not in params
        assert "TILECOL" not in params


def test_wms_request_covers_the_whole_tile_on_the_national_grid():
    _, params = _upstream_query(*_WESTMINSTER)
    min_e, min_n, max_e, max_n = _bbox(params)

    left, bottom, right, top = _web_mercator_bounds(*_WESTMINSTER)
    eastings, northings = _TO_BNG.transform(
        *np.meshgrid(np.linspace(left, right, 9), np.linspace(bottom, top, 9))
    )

    assert min_e < eastings.min()
    assert eastings.max() < max_e
    assert min_n < northings.min()
    assert northings.max() < max_n
    # A margin for resampling, not an oversized request.
    tile_area = np.ptp(eastings) * np.ptp(northings)
    assert (max_e - min_e) * (max_n - min_n) < 1.5 * tile_area


def test_wms_request_matches_the_tile_resolution_with_square_pixels():
    _, params = _upstream_query(*_WESTMINSTER)
    min_e, min_n, max_e, max_n = _bbox(params)
    width, height = int(params["WIDTH"][0]), int(params["HEIGHT"][0])

    # The BBOX is sent to the millimetre, so allow for that rounding.
    assert (max_e - min_e) / width == pytest.approx((max_n - min_n) / height, rel=1e-4)
    assert _TILE_PIXELS <= width <= 300
    assert _TILE_PIXELS <= height <= 300


def test_stale_wms_image_params_are_replaced_not_appended():
    stale = _WMS_BASE + "&BBOX=1,2,3,4&WIDTH=99&HEIGHT=98"
    _, params = _upstream_query(*_WESTMINSTER, wms_base_url=stale)

    assert params["BBOX"] != ["1,2,3,4"]
    assert params["WIDTH"] != ["99"]
    assert params["HEIGHT"] != ["98"]
    assert params["CRS"] == ["EPSG:27700"]
    # Non-image parameters survive untouched, including encoded values.
    assert params["LAYERS"] == ["APGB_Latest_UK_125mm"]
    assert params["FORMAT"] == ["image/jpeg"]


def test_unconfigured_wms_keeps_every_zoom_on_wmts():
    """With no WMS configured the proxy behaves exactly as it did before."""
    parsed, params = _upstream_query(
        21, 4093, 2724, wms_base_url="", wms_min_zoom=AERIAL_WMS_MIN_ZOOM
    )

    assert parsed.path == "/APGB.wmtsx"
    assert params["TILEMATRIX"] == ["21"]


# ---------------------------------------------------------------------------
# WMS reprojection
# ---------------------------------------------------------------------------


def _landmark_wms(landmark):
    """A fake WMS that draws one bright dot at a British National Grid point,
    wherever the requested BBOX places it."""
    client = AsyncMock()

    def stream(_method, url, **_kwargs):
        params = parse_qs(urlsplit(url).query)
        min_e, min_n, max_e, max_n = _bbox(params)
        width, height = int(params["WIDTH"][0]), int(params["HEIGHT"][0])
        col = (landmark[0] - min_e) / (max_e - min_e) * width
        row = (max_n - landmark[1]) / (max_n - min_n) * height

        image = Image.new("RGB", (width, height))
        ImageDraw.Draw(image).ellipse(
            (col - 2, row - 2, col + 2, row + 2), fill="white"
        )
        buffer = io.BytesIO()
        image.save(buffer, format="PNG")
        return _FakeStreamResponse(chunks=[buffer.getvalue()], content_type="image/png")

    client.stream = stream
    return client


async def _fetch_wms_tile(client, z, x, y):
    from app.aerial_proxy.router import _fetch_upstream

    with (
        patch("app.aerial_proxy.router._get_client", return_value=client),
        patch("app.aerial_proxy.router._config") as cfg,
        patch("app.aerial_proxy.router.AERIAL_WMS_MIN_ZOOM", 0),
    ):
        cfg.base_url = _WMTS_BASE
        cfg.wms_base_url = _WMS_BASE
        cfg.max_tile_bytes = 8 * 1024 * 1024
        return await _fetch_upstream(z, x, y)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tile", "fraction"),
    [
        (_WESTMINSTER, (0.3, 0.7)),
        (_WESTMINSTER, (0.85, 0.1)),
        ((14, 8249, 5352), (0.6, 0.4)),
    ],
)
async def test_wms_imagery_lands_on_its_web_mercator_pixel(tile, fraction):
    left, bottom, right, top = _web_mercator_bounds(*tile)
    landmark = _TO_BNG.transform(
        left + fraction[0] * (right - left), top - fraction[1] * (top - bottom)
    )

    result = await _fetch_wms_tile(_landmark_wms(landmark), *tile)

    brightness = np.asarray(
        Image.open(io.BytesIO(result.tile_bytes)).convert("L"), dtype=float
    )
    # The dot spans several pixels, so locate its centroid, not its first
    # bright pixel; the floor drops JPEG ringing from the dark background.
    weight = np.where(brightness > 64, brightness, 0)
    rows, cols = np.indices(weight.shape)
    # +0.5: pixel i covers [i, i + 1), so its centre sits at i + 0.5.
    col = (cols * weight).sum() / weight.sum() + 0.5
    row = (rows * weight).sum() / weight.sum() + 0.5
    assert col == pytest.approx(fraction[0] * _TILE_PIXELS, abs=1)
    assert row == pytest.approx(fraction[1] * _TILE_PIXELS, abs=1)


@pytest.mark.asyncio
async def test_wms_tile_is_served_as_a_web_mercator_sized_jpeg():
    result = await _fetch_wms_tile(_landmark_wms((530000, 180000)), *_WESTMINSTER)

    image = Image.open(io.BytesIO(result.tile_bytes))
    assert result.outcome is TileOutcome.HIT
    assert result.content_type == "image/jpeg"
    assert image.format == "JPEG"
    assert image.size == (_TILE_PIXELS, _TILE_PIXELS)


@pytest.mark.asyncio
async def test_undecodable_wms_image_reports_upstream_error():
    resp = _FakeStreamResponse(chunks=[b"not an image"], content_type="image/jpeg")

    assert await _fetch_wms_tile(_mock_client(resp), *_WESTMINSTER) == _UPSTREAM_ERROR


@pytest.mark.asyncio
async def test_wrongly_sized_wms_image_reports_upstream_error():
    """Dimensions are checked before decoding: a small, highly compressed
    oversized image must not be expanded, and a mis-sized one must not be
    warped on the wrong scale."""
    buffer = io.BytesIO()
    Image.new("RGB", (4000, 4000)).save(buffer, format="PNG")
    resp = _FakeStreamResponse(chunks=[buffer.getvalue()], content_type="image/png")

    with patch.object(Image.Image, "convert") as convert:
        result = await _fetch_wms_tile(_mock_client(resp), *_WESTMINSTER)

    assert result == _UPSTREAM_ERROR
    convert.assert_not_called()


@pytest.mark.asyncio
async def test_missing_wms_image_is_not_reprojected():
    resp = _FakeStreamResponse(status_code=404, chunks=[], content_type="text/plain")

    assert await _fetch_wms_tile(_mock_client(resp), *_WESTMINSTER) == _MISSING
