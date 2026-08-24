"""Aerial tile proxy endpoint.

Relays raster tiles from an upstream WMTS source without exposing the
source URL to the client.  When the upstream tile is unavailable a
lightweight "not available" placeholder is returned instead.

    GET /aerial_proxy/{z}/{x}/{y}
"""

import asyncio
import hashlib
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from threading import Lock
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response

from app.common.http_client import create_async_client
from app.config import AerialProxyConfig

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


def _build_upstream_url(z: int, x: int, y: int) -> str:
    """Set the tile coordinates on the configured upstream WMTS URL."""
    parsed = urlsplit(_config.base_url)
    params = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() not in _TILE_QUERY_KEYS
    ]
    params += [("TILEMATRIX", str(z)), ("TILEROW", str(y)), ("TILECOL", str(x))]
    return urlunsplit(parsed._replace(query=urlencode(params)))


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
        upstream_url = _build_upstream_url(z, x, y)
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

    return TileResult(TileOutcome.HIT, tile_bytes, content_type)


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
