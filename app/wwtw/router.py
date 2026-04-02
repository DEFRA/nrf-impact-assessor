"""Nearby WWTW endpoint.

Accepts an RLB geometry (GeoJSON) and returns all WWTW catchment polygons
where any part of the polygon is within a given distance of the centre of
the RLB polygon.
"""

import logging
import threading

import pandas as pd
from fastapi import APIRouter, HTTPException
from geoalchemy2.functions import (
    ST_Centroid,
    ST_Distance,
    ST_DWithin,
    ST_GeomFromText,
    ST_SetSRID,
    ST_Transform,
)
from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel
from shapely.geometry import shape
from sqlalchemy import select, text

from app.config import DatabaseSettings
from app.models.db import LookupTable, SpatialLayer
from app.models.enums import SpatialLayerType
from app.repositories.engine import create_db_engine
from app.repositories.repository import Repository

logger = logging.getLogger(__name__)

router = APIRouter()

# ---------------------------------------------------------------------------
# Lazy-initialised repository singleton
# ---------------------------------------------------------------------------
_repository: Repository | None = None


def _get_repository() -> Repository:
    """Get or create the module-level Repository singleton."""
    global _repository
    if _repository is None:
        logger.info("Initialising Repository for /wwtw/nearby endpoint...")
        db_settings = DatabaseSettings()
        engine = create_db_engine(db_settings, pool_size=2, max_overflow=2)
        _repository = Repository(engine)
        logger.info("Repository initialised")
    return _repository


# ---------------------------------------------------------------------------
# Lookup cache (same pattern as nutrient.py)
# ---------------------------------------------------------------------------
_lookup_cache: dict[tuple[str, int], pd.DataFrame] = {}
_lookup_cache_lock = threading.Lock()


def _load_wwtw_lookup(repository: Repository) -> pd.DataFrame:
    """Return the wwtw_lookup table as a DataFrame, using a process-level cache."""
    name = "wwtw_lookup"

    with repository.session() as session:
        row = session.execute(
            text(
                "SELECT MAX(version) FROM nrf_reference.lookup_table WHERE name = :name"
            ),
            {"name": name},
        ).fetchone()
    version = row[0] if row and row[0] is not None else 1

    cache_key = (name, version)
    with _lookup_cache_lock:
        if cache_key in _lookup_cache:
            return _lookup_cache[cache_key]

    stmt = (
        select(LookupTable)
        .where(LookupTable.name == name, LookupTable.version == version)
        .limit(1)
    )

    with repository.session() as session:
        obj = session.execute(stmt).scalars().first()

    df = pd.DataFrame(obj.data)
    df["wwtw_code"] = pd.to_numeric(df["wwtw_code"], errors="coerce").astype("Int64")
    df = df.drop_duplicates(subset=["wwtw_code"])

    with _lookup_cache_lock:
        _lookup_cache[cache_key] = df

    return df


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------
_DEFAULT_DISTANCE_M = 10_000.0
_TARGET_SRID = 27700


class NearbyWasteWaterTreatmentWorksRequest(BaseModel):
    """Request body for the nearby WWTW endpoint."""

    geometry: dict = Field(description="RLB geometry as GeoJSON dict")
    srid: int = Field(
        default=4326,
        description="SRID of the input geometry (default 4326 / WGS84)",
    )
    max_distance_m: float = Field(
        default=_DEFAULT_DISTANCE_M,
        gt=0,
        description="Search radius in metres (default 10km)",
    )


class NearbyWasteWaterTreatmentWorksItem(BaseModel):
    """A single nearby WWTW in the response."""

    model_config = ConfigDict(
        frozen=True, alias_generator=to_camel, populate_by_name=True
    )

    wwtw_id: str = Field(description="WwTW identifier")
    wwtw_name: str = Field(description="WwTW facility name")
    distance_km: float = Field(
        ge=0,
        description="Distance in km from centre of RLB to nearest edge of catchment",
    )


class NearbyWasteWaterTreatmentWorksResponse(BaseModel):
    """Response from the nearby WWTW endpoint."""

    model_config = ConfigDict(
        frozen=True, alias_generator=to_camel, populate_by_name=True
    )

    nearby_wwtws: list[NearbyWasteWaterTreatmentWorksItem] = Field(
        description="WWTWs within search radius, ordered by distance"
    )


# ---------------------------------------------------------------------------
# Spatial query
# ---------------------------------------------------------------------------
def _find_nearby_wwtws(
    rlb_wkt: str,
    repository: Repository,
    max_distance_m: float = _DEFAULT_DISTANCE_M,
    srid: int = 4326,
) -> list[dict]:
    """Query PostGIS for WWTW catchments within max_distance_m of the RLB centre."""
    input_geom = ST_SetSRID(ST_GeomFromText(rlb_wkt), srid)
    if srid != _TARGET_SRID:
        input_geom = ST_Transform(input_geom, _TARGET_SRID)

    # Distance is measured from the centre of the RLB to the nearest edge
    # of each WWTW catchment polygon.
    rlb_centroid = ST_Centroid(input_geom)

    stmt = (
        select(
            SpatialLayer.attributes["WwTw_ID"].astext.label("wwtw_id"),
            ST_Distance(SpatialLayer.geometry, rlb_centroid).label("distance_m"),
        )
        .where(
            SpatialLayer.layer_type == SpatialLayerType.WWTW_CATCHMENTS,
            ST_DWithin(SpatialLayer.geometry, rlb_centroid, max_distance_m),
        )
        .order_by("distance_m")
    )

    with repository.session() as session:
        rows = session.execute(stmt).fetchall()

    return [
        {"wwtw_id": str(row.wwtw_id), "distance_m": row.distance_m or 0.0}
        for row in rows
    ]


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------
@router.post(
    "/wwtw/nearby",
    response_model_by_alias=True,
    responses={
        400: {"description": "Invalid geometry"},
    },
)
async def nearby_wwtws(
    body: NearbyWasteWaterTreatmentWorksRequest,
) -> NearbyWasteWaterTreatmentWorksResponse:
    """Find WWTW catchments within a given distance of an RLB polygon."""
    try:
        geom = shape(body.geometry)
    except Exception:
        raise HTTPException(
            status_code=400, detail="Invalid GeoJSON geometry"
        ) from None

    if geom.is_empty:
        raise HTTPException(status_code=400, detail="Geometry is empty")

    rlb_wkt = geom.wkt
    repository = _get_repository()

    nearby = _find_nearby_wwtws(rlb_wkt, repository, body.max_distance_m, body.srid)

    # Enrich with names from lookup table
    lookup_df = _load_wwtw_lookup(repository)
    lookup_map = dict(
        zip(
            lookup_df["wwtw_code"].astype(str),
            lookup_df["wwtw_name"],
            strict=False,
        )
    )

    items = []
    for row in nearby:
        wwtw_id = row["wwtw_id"]
        wwtw_name = lookup_map.get(wwtw_id, f"WWTW {wwtw_id}")
        distance_km = round(row["distance_m"] / 1000.0, 1)
        items.append(
            NearbyWasteWaterTreatmentWorksItem(
                wwtw_id=wwtw_id,
                wwtw_name=wwtw_name,
                distance_km=distance_km,
            )
        )

    return NearbyWasteWaterTreatmentWorksResponse(nearby_wwtws=items)
