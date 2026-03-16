"""XYZ vector tile endpoint.

Serves spatial reference layers as Mapbox Vector Tiles (MVT) via:
    GET /tiles/{layer}/{z}/{x}/{y}.mvt
"""

import hashlib
import logging
import threading
import time
from collections import OrderedDict

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response
from sqlalchemy import text

from app.config import DatabaseSettings, TileServerConfig
from app.models.enums import SpatialLayerType
from app.repositories.engine import create_db_engine
from app.repositories.repository import Repository

logger = logging.getLogger(__name__)

router = APIRouter()

# Validated whitelist: URL slug → SpatialLayerType enum
# The slug (not raw user input) is used as the MVT layer label in SQL.
TILE_LAYERS: dict[str, SpatialLayerType] = {
    "nn_catchments": SpatialLayerType.NN_CATCHMENTS,
    "lpa_boundaries": SpatialLayerType.LPA_BOUNDARIES,
    "wwtw_catchments": SpatialLayerType.WWTW_CATCHMENTS,
    "subcatchments": SpatialLayerType.SUBCATCHMENTS,
}

_tile_config = TileServerConfig()

# ---------------------------------------------------------------------------
# Module-level singletons
# ---------------------------------------------------------------------------

_repository: Repository | None = None
_repository_lock = threading.Lock()

# Per-layer resolved version cache: SpatialLayerType → (version, expiry)
_version_cache: dict[SpatialLayerType, tuple[int, float]] = {}

# In-process LRU tile cache: (layer_slug, z, x, y, version) → (bytes, expiry)
_tile_cache: OrderedDict[tuple, tuple[bytes, float]] = OrderedDict()

# ---------------------------------------------------------------------------
# Prepared SQL (reusable text() clause)
# ---------------------------------------------------------------------------

_TILE_SQL = text("""
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
        FROM nrf_reference.spatial_layer sl
        WHERE sl.layer_type = CAST(:layer_type_name AS nrf_reference.spatial_layer_type)
          AND sl.version = :version
          AND ST_Intersects(
                sl.geometry,
                ST_Transform(ST_TileEnvelope(:z, :x, :y), 27700)
              )
    ) q
    WHERE q.geom IS NOT NULL
""")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_repository() -> Repository:
    """Get or create the module-level Repository singleton (thread-safe)."""
    global _repository
    if _repository is not None:
        return _repository
    with _repository_lock:
        if _repository is None:
            logger.info("Initialising Repository for /tiles endpoint...")
            db_settings = DatabaseSettings()
            engine = create_db_engine(
                db_settings,
                pool_size=_tile_config.db_pool_size,
                max_overflow=_tile_config.db_max_overflow,
            )
            _repository = Repository(engine)
            logger.info("Repository initialised")
        return _repository


def _resolve_layer_version(layer_type: SpatialLayerType) -> int:
    """Return the current max version for the given layer, cached with TTL."""
    now = time.monotonic()

    if layer_type in _version_cache:
        version, expiry = _version_cache[layer_type]
        if now < expiry:
            return version

    repo = _get_repository()
    with repo.engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT MAX(version) FROM nrf_reference.spatial_layer "
                "WHERE layer_type = CAST(:layer_type_name AS nrf_reference.spatial_layer_type)"
            ),
            {"layer_type_name": layer_type.name},
        ).fetchone()

    version = row[0] if row and row[0] is not None else 1
    _version_cache[layer_type] = (version, now + _tile_config.version_ttl_seconds)
    return version


def _query_tile(
    z: int,
    x: int,
    y: int,
    layer_type: SpatialLayerType,
    layer_name: str,
    version: int,
) -> bytes:
    """Execute the MVT SQL query and return raw tile bytes."""
    repo = _get_repository()
    with repo.engine.connect() as conn:
        row = conn.execute(
            _TILE_SQL,
            {
                "layer_name": layer_name,
                "z": z,
                "x": x,
                "y": y,
                "layer_type_name": layer_type.name,
                "version": version,
            },
        ).fetchone()
    return bytes(row[0]) if row and row[0] else b""


def _get_tile(layer_slug: str, z: int, x: int, y: int) -> bytes:
    """Return tile bytes from cache, or query PostGIS on a cache miss."""
    layer_type = TILE_LAYERS[layer_slug]
    version = _resolve_layer_version(layer_type)
    cache_key = (layer_slug, z, x, y, version)
    now = time.monotonic()

    if cache_key in _tile_cache:
        tile_bytes, expiry = _tile_cache[cache_key]
        if now < expiry:
            _tile_cache.move_to_end(cache_key)
            return tile_bytes
        del _tile_cache[cache_key]

    tile_bytes = _query_tile(z, x, y, layer_type, layer_slug, version)

    # Evict oldest entry when at capacity
    while len(_tile_cache) >= _tile_config.cache_max_size:
        _tile_cache.popitem(last=False)

    _tile_cache[cache_key] = (tile_bytes, now + _tile_config.cache_ttl_seconds)
    return tile_bytes


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
    if layer not in TILE_LAYERS:
        raise HTTPException(status_code=404, detail="Unknown layer")

    if z < _tile_config.min_zoom or z > _tile_config.max_zoom:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Zoom level {z} is outside the allowed range "
                f"[{_tile_config.min_zoom}, {_tile_config.max_zoom}]"
            ),
        )

    tile_bytes = _get_tile(layer, z, x, y)

    version = _version_cache.get(TILE_LAYERS[layer], (1, 0.0))[0]
    etag = hashlib.md5(  # noqa: S324
        f"{layer}:{z}:{x}:{y}:{version}".encode()
    ).hexdigest()
    quoted_etag = f'"{etag}"'

    if request.headers.get("if-none-match") == quoted_etag:
        return Response(
            status_code=304,
            headers={"ETag": quoted_etag},
        )

    return Response(
        content=tile_bytes,
        media_type="application/vnd.mapbox-vector-tile",
        headers={
            "Cache-Control": "public, max-age=3600",
            "ETag": quoted_etag,
        },
    )