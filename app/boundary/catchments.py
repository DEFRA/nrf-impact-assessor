"""NN catchment lookup for a boundary.

Shared by /check-boundary and by the assessor's PATCH callback.
"""

import geopandas as gpd
from geoalchemy2.functions import (
    ST_Area,
    ST_CollectionExtract,
    ST_GeomFromText,
    ST_Intersection,
    ST_Intersects,
    ST_SetSRID,
    ST_Union,
)
from sqlalchemy import func, select

from app.data_sync.active_version import get_active_version
from app.models.db import NnCatchments
from app.repositories.repository import Repository


def find_intersecting_catchments(
    gdf: gpd.GeoDataFrame, repository: Repository
) -> list[dict]:
    """Query PostGIS for the NN catchments the uploaded boundary falls in.

    `catchmentOverlapPercentage` is the share of the *boundary* in each
    catchment, same denominator as the sibling `overlapPercentage`.

    """
    input_union = gdf.union_all()
    input_area_sqm = input_union.area

    input_geom = ST_SetSRID(ST_GeomFromText(input_union.wkt), 27700)
    intersection = ST_CollectionExtract(
        ST_Intersection(NnCatchments.geometry, input_geom), 3
    )
    label = NnCatchments.attributes["Label"].astext
    oid = NnCatchments.attributes["OID"].astext

    with repository.session() as session:
        version = get_active_version(session, "nn_catchments")
        overlap_area = ST_Area(ST_Union(intersection))
        stmt = (
            select(
                label.label("label"),
                overlap_area.label("overlap_area_sqm"),
                func.min(oid).label("catchment_id"),
            )
            .where(
                NnCatchments.version == version,
                ST_Intersects(NnCatchments.geometry, input_geom),
            )
            .group_by(label)
            # ST_Intersects is true for an edge-only touch, which has no area.
            .having(overlap_area > 0)
        )
        rows = session.execute(stmt).fetchall()

    results = []
    for row in rows:
        if not row.label or not row.label.strip():
            continue
        area_sqm = row.overlap_area_sqm or 0.0
        results.append(
            {
                "label": row.label.strip(),
                "catchmentOverlapPercentage": round(
                    (area_sqm / input_area_sqm) * 100, 2
                )
                if input_area_sqm > 0
                else 0.0,
                "catchmentId": row.catchment_id,
            }
        )
    return sorted(results, key=lambda c: c["label"])
