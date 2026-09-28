"""Aerial tile proxy endpoint.

Relays raster tiles from an upstream WMTS source without exposing the
source URL to the client.  Close-in tiles are instead rendered from the
upstream WMS in British National Grid and reprojected here to Web Mercator.
When the upstream tile is unavailable a lightweight "not available"
placeholder is returned instead.

    GET /aerial_proxy/{z}/{x}/{y}
"""

import asyncio
import hashlib
import io
import logging
import math
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from threading import Lock
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx
import numpy as np
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response
from PIL import Image
from pyproj import Transformer

from app.common.http_client import create_async_client
from app.config import AERIAL_WMS_MIN_ZOOM, AerialProxyConfig

logger = logging.getLogger(__name__)

router = APIRouter()

_config = AerialProxyConfig()

# ---------------------------------------------------------------------------
# Placeholder tile: 256x256 grey PNG with white "No imagery available" text.
# Loaded once at import time from the adjacent file.
# ---------------------------------------------------------------------------

_NOT_AVAILABLE_TILE: bytes = (
    Path(__file__).with_name("no_imagery_available.png").read_bytes()
)

_ALLOWED_MEDIA_TYPES = frozenset({"image/jpeg", "image/png"})


def _entity_tag(payload: bytes) -> str:
    """Strong validator over the exact bytes sent to the client."""
    return f'"{hashlib.sha256(payload).hexdigest()}"'


_NOT_AVAILABLE_ETAG = _entity_tag(_NOT_AVAILABLE_TILE)

# ---------------------------------------------------------------------------
# Tile outcomes
#
# The placeholder is served for both MISSING and UPSTREAM_ERROR so the map
# degrades gracefully, but the two are cached for different lengths of time
# and reported separately in the X-Aerial-Proxy-Tile response header.
# ---------------------------------------------------------------------------


class TileOutcome(StrEnum):
    HIT = "hit"
    MISSING = "missing"
    UPSTREAM_ERROR = "upstream-error"


@dataclass(frozen=True)
class TileResult:
    outcome: TileOutcome
    tile_bytes: bytes | None = None
    content_type: str | None = None
    #: Validator for the bytes this result actually sends — the tile itself
    #: for a HIT, the placeholder otherwise.  Hashed once per fetch rather
    #: than per request, since results are cached.
    etag: str = field(init=False)

    def __post_init__(self) -> None:
        payload = self.tile_bytes if self.outcome is TileOutcome.HIT else None
        object.__setattr__(
            self, "etag", _entity_tag(payload) if payload else _NOT_AVAILABLE_ETAG
        )

    @property
    def ttl_seconds(self) -> int:
        if self.outcome is TileOutcome.HIT:
            return _config.cache_ttl_seconds
        if self.outcome is TileOutcome.MISSING:
            return _config.missing_cache_ttl_seconds
        return _config.error_cache_ttl_seconds

    @property
    def size_bytes(self) -> int:
        return len(self.tile_bytes) if self.tile_bytes else 0


_MISSING = TileResult(TileOutcome.MISSING)
_UPSTREAM_ERROR = TileResult(TileOutcome.UPSTREAM_ERROR)

# ---------------------------------------------------------------------------
# Shared async HTTP client (lazy singleton)
# ---------------------------------------------------------------------------

_shared_client: httpx.AsyncClient | None = None


def _get_client() -> httpx.AsyncClient:
    global _shared_client
    if _shared_client is None:
        _shared_client = create_async_client(request_timeout=_config.timeout_seconds)
    return _shared_client


async def close_client() -> None:
    global _shared_client
    if _shared_client is not None:
        await _shared_client.aclose()
        _shared_client = None


# ---------------------------------------------------------------------------
# In-process LRU tile cache
#
# Bounded by BOTH entry count (cache_max_size) and total payload bytes
# (cache_max_bytes) — the count alone would allow cache_max_size *
# max_tile_bytes of resident memory per worker.
# ---------------------------------------------------------------------------

_tile_cache: OrderedDict[tuple, tuple[TileResult, float]] = OrderedDict()
_tile_cache_lock = Lock()
_tile_cache_bytes = 0


def _cache_clear() -> None:
    """Drop every entry and reset the byte accounting (used by tests)."""
    global _tile_cache_bytes
    with _tile_cache_lock:
        _tile_cache.clear()
        _tile_cache_bytes = 0


def _drop_locked(key: tuple) -> None:
    """Remove one entry, keeping the byte total in step.  Lock must be held."""
    global _tile_cache_bytes
    entry = _tile_cache.pop(key, None)
    if entry is not None:
        _tile_cache_bytes -= entry[0].size_bytes


def _cache_get(key: tuple) -> TileResult | None:
    now = time.monotonic()
    with _tile_cache_lock:
        if key in _tile_cache:
            result, expiry = _tile_cache[key]
            if now < expiry:
                _tile_cache.move_to_end(key)
                return result
            _drop_locked(key)
    return None


def _cache_put(key: tuple, result: TileResult) -> None:
    global _tile_cache_bytes

    size = result.size_bytes
    if size > _config.cache_max_bytes:
        # Larger than the whole budget: caching it would evict everything
        # else only to be evicted itself by the next insert.
        return

    now = time.monotonic()
    with _tile_cache_lock:
        _drop_locked(key)
        while _tile_cache and (
            len(_tile_cache) >= _config.cache_max_size
            or _tile_cache_bytes + size > _config.cache_max_bytes
        ):
            _drop_locked(next(iter(_tile_cache)))
        _tile_cache[key] = (result, now + result.ttl_seconds)
        _tile_cache_bytes += size


# ---------------------------------------------------------------------------
# Request coalescing (asyncio — one upstream fetch per unique tile)
# ---------------------------------------------------------------------------

_inflight: dict[tuple, asyncio.Task[TileResult]] = {}
_inflight_lock = Lock()


# WMTS KVP parameters that identify the tile.  Any values already present
# in the configured base URL are replaced with the requested coordinates.
_TILE_QUERY_KEYS = frozenset({"tilematrix", "tilerow", "tilecol"})

# The equivalent for WMS GetMap: the parameters that frame the image.
_WMS_QUERY_KEYS = frozenset({"crs", "bbox", "width", "height"})

# Metres from the EPSG:3857 origin to the antimeridian; the XYZ grid divides
# that square into 2**z rows and columns.
_WEB_MERCATOR_ORIGIN = 20037508.342789244

# Matches the WMTS tile size, so the two halves hand over at the same scale.
_WMS_TILE_PIXELS = 256

# The WMS tile is warped through a coarse lattice of exact transforms and
# interpolated between them; the transform barely bends across one tile.
_WARP_LATTICE_STEPS = 16

# Source pixels of margin, so bilinear sampling at the tile edge has
# neighbors to read.
_WARP_MARGIN_PIXELS = 2

_WARP_JPEG_QUALITY = 85

# Built lazily and shared: pyproj resolves the most accurate operation
# available, which is OSTN15 wherever scripts/install_ostn15.py has run.
_to_national_grid: Transformer | None = None


def _national_grid_transformer() -> Transformer:
    global _to_national_grid
    if _to_national_grid is None:
        _to_national_grid = Transformer.from_crs(3857, 27700, always_xy=True)
    return _to_national_grid


@dataclass(frozen=True)
class _SourceWindow:
    """The British National Grid image a Web Mercator tile is warped from.

    `eastings`/`northings` hold the national grid position of every output
    pixel centre, so warping is a lookup into the fetched image.
    """

    min_e: float
    max_n: float
    resolution: float
    width: int
    height: int
    eastings: np.ndarray = field(repr=False)
    northings: np.ndarray = field(repr=False)

    @property
    def bbox(self) -> str:
        """WMS 1.3.0 BBOX; EPSG:27700 is easting/northing ordered."""
        max_e = self.min_e + self.width * self.resolution
        min_n = self.max_n - self.height * self.resolution
        return f"{self.min_e:.3f},{min_n:.3f},{max_e:.3f},{self.max_n:.3f}"


def _with_query_params(
    base_url: str, replaced: frozenset[str], params: list[tuple[str, str]]
) -> str:
    """Drop `replaced` from `base_url`'s query, then append `params`.

    Dropping first overwrites stale coordinates left in the configured URL,
    rather than sending two values for the same parameter.
    """
    parsed = urlsplit(base_url)
    kept = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() not in replaced
    ]
    return urlunsplit(parsed._replace(query=urlencode(kept + params)))


def _upsample(lattice: np.ndarray, size: int) -> np.ndarray:
    """Bilinearly interpolate a square lattice spanning a tile's edges to
    the centres of its `size` x `size` pixels."""
    steps = lattice.shape[0] - 1
    position = (np.arange(size) + 0.5) * steps / size
    lower = np.minimum(position.astype(int), steps - 1)
    frac = position - lower
    rows = lattice[lower] * (1 - frac)[:, None] + lattice[lower + 1] * frac[:, None]
    return rows[:, lower] * (1 - frac) + rows[:, lower + 1] * frac


def _wms_source_window(z: int, x: int, y: int) -> _SourceWindow:
    """The national grid window covering one XYZ tile, at about its resolution."""
    span = 2 * _WEB_MERCATOR_ORIGIN / (2**z)
    left = -_WEB_MERCATOR_ORIGIN + x * span
    top = _WEB_MERCATOR_ORIGIN - y * span

    edges = np.linspace(0.0, span, _WARP_LATTICE_STEPS + 1)
    lattice_e, lattice_n = _national_grid_transformer().transform(
        *np.meshgrid(left + edges, top - edges)
    )

    # Ground size of one output pixel along the tile's top edge.
    resolution = (
        math.dist(
            (lattice_e[0, 0], lattice_n[0, 0]), (lattice_e[0, -1], lattice_n[0, -1])
        )
        / _WMS_TILE_PIXELS
    )
    margin = _WARP_MARGIN_PIXELS * resolution
    min_e = lattice_e.min() - margin
    max_n = lattice_n.max() + margin

    return _SourceWindow(
        min_e=min_e,
        max_n=max_n,
        resolution=resolution,
        width=math.ceil((lattice_e.max() + margin - min_e) / resolution),
        height=math.ceil((max_n - (lattice_n.min() - margin)) / resolution),
        eastings=_upsample(lattice_e, _WMS_TILE_PIXELS),
        northings=_upsample(lattice_n, _WMS_TILE_PIXELS),
    )


def _use_wms(z: int) -> bool:
    """Whether this zoom is served by the WMS rather than the WMTS.

    The WMTS pyramid is a fixed grid and caches well, so it takes the low
    zooms; close in, the WMS renders from the native imagery instead.
    """
    return bool(_config.wms_base_url) and z >= AERIAL_WMS_MIN_ZOOM


def _build_upstream_url(z: int, x: int, y: int) -> tuple[str, _SourceWindow | None]:
    """Point the configured upstream URL at one tile.

    WMS tiles come back with the window to warp them from; WMTS tiles are
    already Web Mercator and are served as they come.
    """
    if _use_wms(z):
        window = _wms_source_window(z, x, y)
        url = _with_query_params(
            _config.wms_base_url,
            _WMS_QUERY_KEYS,
            [
                # Getmapping's own Web Mercator rendering sits metres off the
                # OS basemap, so ask for the native grid and reproject here.
                ("CRS", "EPSG:27700"),
                ("BBOX", window.bbox),
                ("WIDTH", str(window.width)),
                ("HEIGHT", str(window.height)),
            ],
        )
        return url, window
    url = _with_query_params(
        _config.base_url,
        _TILE_QUERY_KEYS,
        [("TILEMATRIX", str(z)), ("TILEROW", str(y)), ("TILECOL", str(x))],
    )
    return url, None


def _warp_to_web_mercator(window: _SourceWindow, image_bytes: bytes) -> bytes:
    """Bilinearly resample a national grid image onto the Web Mercator tile."""
    with Image.open(io.BytesIO(image_bytes)) as image:
        # Checked from the header before decoding: the byte cap bounds the
        # compressed body, not the raster, and a mis-sized image would be
        # warped on the wrong scale.
        if image.size != (window.width, window.height):
            message = (
                f"WMS image is {image.size}, requested {(window.width, window.height)}"
            )
            raise ValueError(message)
        source = np.asarray(image.convert("RGB"), dtype=np.float32)
    height, width = source.shape[:2]

    # Source pixel coordinates of each output pixel centre.
    col = (window.eastings - window.min_e) / window.resolution - 0.5
    row = (window.max_n - window.northings) / window.resolution - 0.5
    col = np.clip(col, 0, width - 1)
    row = np.clip(row, 0, height - 1)
    col0 = np.minimum(col.astype(int), width - 2)
    row0 = np.minimum(row.astype(int), height - 2)
    fc = (col - col0)[..., None]
    fr = (row - row0)[..., None]

    top = source[row0, col0] * (1 - fc) + source[row0, col0 + 1] * fc
    bottom = source[row0 + 1, col0] * (1 - fc) + source[row0 + 1, col0 + 1] * fc
    pixels = np.rint(top * (1 - fr) + bottom * fr).astype(np.uint8)

    output = io.BytesIO()
    Image.fromarray(pixels).save(output, format="JPEG", quality=_WARP_JPEG_QUALITY)
    return output.getvalue()


async def _read_capped(resp: httpx.Response, tile_ref: str) -> bytes | None:
    """Read the response body, giving up once it exceeds the size cap."""
    limit = _config.max_tile_bytes
    chunks: list[bytes] = []
    total = 0
    async for chunk in resp.aiter_bytes():
        total += len(chunk)
        if total > limit:
            logger.error(
                "Aerial proxy upstream body exceeds %d bytes for %s", limit, tile_ref
            )
            return None
        chunks.append(chunk)
    return b"".join(chunks)


async def _fetch_upstream(z: int, x: int, y: int) -> TileResult:
    tile_ref = f"{z}/{x}/{y}"

    try:
        upstream_url, window = _build_upstream_url(z, x, y)
        async with _get_client().stream(
            "GET", upstream_url, follow_redirects=True
        ) as resp:
            if resp.status_code == 404:
                return _MISSING

            if not (200 <= resp.status_code < 300):
                logger.error(
                    "Aerial proxy upstream error %d for %s", resp.status_code, tile_ref
                )
                return _UPSTREAM_ERROR

            # An absent content-type is not assumed to be imagery: an
            # untyped HTML error page must not be cached as a tile.
            content_type = resp.headers.get("content-type", "")
            media_type = content_type.split(";", 1)[0].strip().lower()
            if media_type not in _ALLOWED_MEDIA_TYPES:
                logger.error(
                    "Aerial proxy unexpected content-type %r for %s",
                    content_type,
                    tile_ref,
                )
                return _UPSTREAM_ERROR

            declared_length = resp.headers.get("content-length")
            if (
                declared_length
                and declared_length.isdigit()
                and int(declared_length) > _config.max_tile_bytes
            ):
                logger.error(
                    "Aerial proxy upstream declared %s bytes (cap %d) for %s",
                    declared_length,
                    _config.max_tile_bytes,
                    tile_ref,
                )
                return _UPSTREAM_ERROR

            tile_bytes = await _read_capped(resp, tile_ref)
            if tile_bytes is None:
                return _UPSTREAM_ERROR
    except httpx.TimeoutException:
        logger.error("Aerial proxy upstream timeout: %s", tile_ref)
        return _UPSTREAM_ERROR
    except Exception:
        # Includes httpx.HTTPError and httpx.InvalidURL (a misconfigured
        # base_url); never let it surface as a 500 to every waiter.
        logger.exception("Aerial proxy upstream request failed: %s", tile_ref)
        return _UPSTREAM_ERROR

    if not tile_bytes:
        return _MISSING

    if window is None:
        return TileResult(TileOutcome.HIT, tile_bytes, content_type)

    try:
        # Off the event loop: numpy and Pillow release the GIL for the bulk.
        warped = await asyncio.to_thread(_warp_to_web_mercator, window, tile_bytes)
    except Exception:
        logger.exception("Aerial proxy could not reproject WMS image: %s", tile_ref)
        return _UPSTREAM_ERROR

    return TileResult(TileOutcome.HIT, warped, "image/jpeg")


async def _get_tile(z: int, x: int, y: int) -> TileResult:
    key = (z, x, y)

    cached = _cache_get(key)
    if cached is not None:
        return cached

    with _inflight_lock:
        task = _inflight.get(key)
        if task is None:
            task = asyncio.ensure_future(_fetch_and_cache(z, x, y))
            _inflight[key] = task

    # Shield the shared task from this waiter's own cancellation
    return await asyncio.shield(task)


async def _fetch_and_cache(z: int, x: int, y: int) -> TileResult:
    key = (z, x, y)
    try:
        # Negative results are cached too (on their own shorter TTLs) so a
        # region with no coverage does not re-hit the upstream on every pan.
        result = await _fetch_upstream(z, x, y)
        _cache_put(key, result)
        return result
    finally:
        with _inflight_lock:
            _inflight.pop(key, None)


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------

_OUTCOME_HEADER = "X-Aerial-Proxy-Tile"


def _response_headers(result: TileResult) -> dict[str, str]:
    """Browser cache lifetime mirrors this outcome's server-side TTL."""
    return {
        "Cache-Control": f"private, max-age={result.ttl_seconds}",
        "ETag": result.etag,
        _OUTCOME_HEADER: result.outcome.value,
    }


def _if_none_match_matches(header: str | None, etag: str) -> bool:
    """Weak comparison of an If-None-Match list against our validator.

    RFC 9110 §13.1.2: `*` matches any representation, and the comparison
    ignores the `W/` weakness prefix on either side.
    """
    if not header:
        return False
    if header.strip() == "*":
        return True
    for candidate in header.split(","):
        candidate = candidate.strip()
        if candidate.startswith("W/"):
            candidate = candidate[2:]
        if candidate == etag:
            return True
    return False


@router.get(
    "/aerial_proxy/{z}/{x}/{y}",
    responses={
        200: {"content": {"image/jpeg": {}, "image/png": {}}},
        304: {"description": "Client's cached tile is still current"},
        502: {"description": "Aerial proxy not configured"},
    },
)
async def proxy_aerial_tile(request: Request, z: int, x: int, y: int) -> Response:
    """Proxy a raster tile from the upstream aerial source.

    `z`/`x`/`y` map to the WMTS TILEMATRIX/TILECOL/TILEROW parameters.

    Returns the upstream tile on success.  When the tile cannot be served
    a placeholder PNG is returned with status 200 so the map degrades
    gracefully; the `X-Aerial-Proxy-Tile` header distinguishes the cases:

    * ``hit``            — an upstream tile
    * ``missing``        — upstream has no imagery here (404 or empty body)
    * ``upstream-error`` — timeout, connection error, upstream 5xx, a
      missing or unexpected content type, or an oversized body (ERROR)

    Every response carries a strong `ETag`; a client revalidating with a
    matching `If-None-Match` gets 304 and refreshed cache directives
    instead of the payload.

    502 is returned only when the proxy has no `base_url` configured.
    """
    if not _config.base_url:
        raise HTTPException(status_code=502, detail="Aerial proxy not configured")

    result = await _get_tile(z, x, y)
    headers = _response_headers(result)

    if _if_none_match_matches(request.headers.get("if-none-match"), result.etag):
        return Response(status_code=304, headers=headers)

    if result.outcome is not TileOutcome.HIT:
        return Response(
            content=_NOT_AVAILABLE_TILE,
            media_type="image/png",
            headers=headers,
        )

    return Response(
        content=result.tile_bytes,
        media_type=result.content_type,
        headers=headers,
    )
