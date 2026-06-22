"""XYZ vector tile endpoint.

Serves spatial reference layers as Mapbox Vector Tiles (MVT) via:
    GET /tiles/{layer}/{z}/{x}/{y}.mvt
"""

import dataclasses
import hashlib
import itertools
import logging
import threading
import time
from collections import OrderedDict

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response
from sqlalchemy import text

from app.config import TileServerConfig
from app.repositories.engine import get_shared_repository
from app.repositories.repository import Repository

logger = logging.getLogger(__name__)

router = APIRouter()

# Validated whitelist: URL slug → qualified table name in public schema.
# The slug (not raw user input) is used as the MVT layer label in SQL.
TILE_LAYERS: dict[str, str] = {
    "nn_catchments": "public.nn_catchments",
    "lpa_boundaries": "public.lpa_boundaries",
    "wwtw_catchments": "public.wwtw_catchments",
    "subcatchments": "public.subcatchments",
}

EDP_TILE_LAYERS: frozenset[str] = frozenset({"edp_boundaries"})

_tile_config = TileServerConfig()

# ---------------------------------------------------------------------------
# Module-level singletons
# ---------------------------------------------------------------------------

# Per-layer resolved version cache: slug → (version, expiry)
_version_cache: dict[str, tuple[int, float]] = {}
_version_cache_lock = threading.Lock()

# Version cache for edp_boundary_layer (single table, no layer_type key)
_edp_version_cache: tuple[int, float] | None = None

# In-process LRU tile cache: (layer_slug, z, x, y, version) → (bytes, expiry)
_tile_cache: OrderedDict[tuple, tuple[bytes, float]] = OrderedDict()
_tile_cache_lock = threading.Lock()

# Monotonic request counter for 1-in-N sampling of cheap cache-hit timing logs.
_log_counter = itertools.count()

# ---------------------------------------------------------------------------
# Prepared SQL (reusable text() clauses)
# ---------------------------------------------------------------------------

_TILE_SQL_TEMPLATE = """
    SELECT ST_AsMVT(q, :layer_name, 4096, 'geom') AS mvt
    FROM (
        SELECT
            ST_AsMVTGeom(
                ST_Transform(sl.geometry, 3857),
                ST_TileEnvelope(:z, :x, :y),
                4096,
                64,
                true
            ) AS geom,
            sl.name,
            sl.attributes
        FROM {table} sl
        WHERE sl.version = :version
          AND ST_Intersects(
                sl.geometry,
                ST_Transform(ST_TileEnvelope(:z, :x, :y), 27700)
              )
    ) q
    WHERE q.geom IS NOT NULL
"""

_EDP_TILE_SQL = text("""
    SELECT ST_AsMVT(q, :layer_name, 4096, 'geom') AS mvt
    FROM (
        SELECT
            ST_AsMVTGeom(
                ST_Transform(edp.geometry, 3857),
                ST_TileEnvelope(:z, :x, :y),
                4096,
                64,
                true
            ) AS geom,
            edp.name,
            edp.attributes
        FROM public.edp_boundary_layer edp
        WHERE edp.version = :version
          AND ST_Intersects(
                edp.geometry,
                ST_Transform(ST_TileEnvelope(:z, :x, :y), 27700)
              )
    ) q
    WHERE q.geom IS NOT NULL
""")


# ---------------------------------------------------------------------------
# Timing
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class TileTimings:
    """Per-request phase timings (milliseconds) for tile generation.

    Phases isolate where wall-clock time goes so a slow tile can be attributed:
      - ``connect_ms``: pool checkout. A spike here means the pooled socket was
        dropped while idle and ``pool_pre_ping`` forced a TCP+TLS+IAM reconnect.
      - ``query_ms``: server-side PostGIS execution + network transfer of the
        result bytea. Compare against ``size_bytes`` to separate compute from
        transfer.
    """

    version_ms: float = 0.0
    cache_ms: float = 0.0
    connect_ms: float = 0.0
    query_ms: float = 0.0
    total_ms: float = 0.0
    cache_hit: bool = False
    size_bytes: int = 0

    def server_timing_header(self) -> str:
        """Render as an HTTP ``Server-Timing`` header value (visible in devtools)."""
        return ", ".join(
            (
                f"version;dur={self.version_ms:.1f}",
                f"cache;dur={self.cache_ms:.3f}",
                f"connect;dur={self.connect_ms:.1f}",
                f"query;dur={self.query_ms:.1f}",
                f"total;dur={self.total_ms:.1f}",
                f"size;desc=bytes;dur={self.size_bytes}",
            )
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_repository() -> Repository:
    """Return the process-wide shared Repository."""
    return get_shared_repository()


def _resolve_layer_version(slug: str) -> int:
    """Return the current max version for the given layer, cached with TTL."""
    now = time.monotonic()

    with _version_cache_lock:
        if slug in _version_cache:
            version, expiry = _version_cache[slug]
            if now < expiry:
                return version

    table = TILE_LAYERS[slug]
    repo = _get_repository()
    with repo.engine.connect() as conn:
        row = conn.execute(
            text(f"SELECT MAX(version) FROM {table}"),  # noqa: S608
        ).fetchone()

    version = row[0] if row and row[0] is not None else 1
    with _version_cache_lock:
        _version_cache[slug] = (version, now + _tile_config.version_ttl_seconds)
    return version


def _resolve_edp_version() -> int:
    """Return the current max version for edp_boundary_layer, cached with TTL."""
    global _edp_version_cache
    now = time.monotonic()

    with _version_cache_lock:
        if _edp_version_cache is not None:
            version, expiry = _edp_version_cache
            if now < expiry:
                return version

    repo = _get_repository()
    with repo.engine.connect() as conn:
        row = conn.execute(
            text("SELECT MAX(version) FROM public.edp_boundary_layer")
        ).fetchone()

    version = row[0] if row and row[0] is not None else 1
    with _version_cache_lock:
        _edp_version_cache = (version, now + _tile_config.version_ttl_seconds)
    return version


def _query_tile(
    z: int,
    x: int,
    y: int,
    slug: str,
    layer_name: str,
    version: int,
    timings: TileTimings,
) -> bytes:
    """Execute the MVT SQL query against the dedicated layer table and return raw tile bytes."""
    table = TILE_LAYERS[slug]
    sql = text(_TILE_SQL_TEMPLATE.format(table=table))
    repo = _get_repository()
    t0 = time.perf_counter()
    with repo.engine.connect() as conn:
        timings.connect_ms += (time.perf_counter() - t0) * 1000
        t1 = time.perf_counter()
        row = conn.execute(
            sql,
            {"layer_name": layer_name, "z": z, "x": x, "y": y, "version": version},
        ).fetchone()
        timings.query_ms += (time.perf_counter() - t1) * 1000
    return bytes(row[0]) if row and row[0] else b""


def _query_edp_tile(
    z: int, x: int, y: int, layer_name: str, version: int, timings: TileTimings
) -> bytes:
    """Execute the MVT SQL query against edp_boundary_layer and return raw tile bytes."""
    repo = _get_repository()
    t0 = time.perf_counter()
    with repo.engine.connect() as conn:
        timings.connect_ms += (time.perf_counter() - t0) * 1000
        t1 = time.perf_counter()
        row = conn.execute(
            _EDP_TILE_SQL,
            {"layer_name": layer_name, "z": z, "x": x, "y": y, "version": version},
        ).fetchone()
        timings.query_ms += (time.perf_counter() - t1) * 1000
    return bytes(row[0]) if row and row[0] else b""


def _get_tile(layer_slug: str, z: int, x: int, y: int) -> tuple[bytes, TileTimings]:
    """Return tile bytes (with phase timings) from cache, or query PostGIS on a miss."""
    timings = TileTimings()
    t_start = time.perf_counter()

    t_version = time.perf_counter()
    if layer_slug in EDP_TILE_LAYERS:
        version = _resolve_edp_version()
    else:
        version = _resolve_layer_version(layer_slug)
    timings.version_ms = (time.perf_counter() - t_version) * 1000

    cache_key = (layer_slug, z, x, y, version)
    now = time.monotonic()

    t_cache = time.perf_counter()
    with _tile_cache_lock:
        if cache_key in _tile_cache:
            tile_bytes, expiry = _tile_cache[cache_key]
            if now < expiry:
                _tile_cache.move_to_end(cache_key)
                timings.cache_hit = True
                timings.cache_ms = (time.perf_counter() - t_cache) * 1000
                timings.size_bytes = len(tile_bytes)
                timings.total_ms = (time.perf_counter() - t_start) * 1000
                return tile_bytes, timings
            del _tile_cache[cache_key]
    timings.cache_ms = (time.perf_counter() - t_cache) * 1000

    if layer_slug in EDP_TILE_LAYERS:
        tile_bytes = _query_edp_tile(z, x, y, layer_slug, version, timings)
    else:
        tile_bytes = _query_tile(z, x, y, layer_slug, layer_slug, version, timings)

    with _tile_cache_lock:
        # Evict oldest entries when at capacity
        while len(_tile_cache) >= _tile_config.cache_max_size:
            _tile_cache.popitem(last=False)
        _tile_cache[cache_key] = (tile_bytes, now + _tile_config.cache_ttl_seconds)

    timings.size_bytes = len(tile_bytes)
    timings.total_ms = (time.perf_counter() - t_start) * 1000
    return tile_bytes, timings


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------


@router.get(
    "/tiles/{layer}/{z}/{x}/{y}.mvt",
    responses={
        200: {"content": {"application/vnd.mapbox-vector-tile": {}}},
        304: {"description": "Not modified"},
        400: {"description": "Invalid zoom level"},
        404: {"description": "Unknown layer"},
    },
)
def get_tile(request: Request, layer: str, z: int, x: int, y: int) -> Response:
    """Return an XYZ Mapbox Vector Tile for the given layer and tile coordinates.

    Returns binary MVT data. An empty tile (no intersecting features) is returned
    as 200 with an empty body — tile clients handle this more reliably than 204.
    """
    if layer not in TILE_LAYERS and layer not in EDP_TILE_LAYERS:
        raise HTTPException(status_code=404, detail="Unknown layer")

    if z < _tile_config.min_zoom or z > _tile_config.max_zoom:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Zoom level {z} is outside the allowed range "
                f"[{_tile_config.min_zoom}, {_tile_config.max_zoom}]"
            ),
        )

    tile_bytes, timings = _get_tile(layer, z, x, y)

    # Always log DB-touching misses and slow requests; sample cheap hits 1-in-N.
    is_slow = timings.total_ms >= _tile_config.log_slow_ms
    is_sampled = next(_log_counter) % _tile_config.log_sample_n == 0
    if not timings.cache_hit or is_slow or is_sampled:
        # Log only the whitelisted slug, never the raw request value, so no
        # user-controlled data reaches the log (CWE-117). z/x/y are ints.
        safe_layer = (
            layer if (layer in TILE_LAYERS or layer in EDP_TILE_LAYERS) else "unknown"
        )
        logger.info(
            "tile %s/%d/%d/%d %s total=%.1fms version=%.1fms cache=%.3fms "
            "connect=%.1fms query=%.1fms size=%dB",
            safe_layer,
            int(z),
            int(x),
            int(y),
            "HIT" if timings.cache_hit else "MISS",
            timings.total_ms,
            timings.version_ms,
            timings.cache_ms,
            timings.connect_ms,
            timings.query_ms,
            timings.size_bytes,
        )

    if layer in EDP_TILE_LAYERS:
        version = _edp_version_cache[0] if _edp_version_cache else 1
    else:
        version = _version_cache.get(layer, (1, 0.0))[0]
    etag = hashlib.sha256(f"{layer}:{z}:{x}:{y}:{version}".encode()).hexdigest()
    quoted_etag = f'"{etag}"'

    server_timing = timings.server_timing_header()

    if request.headers.get("if-none-match") == quoted_etag:
        return Response(
            status_code=304,
            headers={"ETag": quoted_etag, "Server-Timing": server_timing},
        )

    return Response(
        content=tile_bytes,
        media_type="application/vnd.mapbox-vector-tile",
        headers={
            "Cache-Control": "public, max-age=3600",
            "ETag": quoted_etag,
            "Server-Timing": server_timing,
        },
    )
