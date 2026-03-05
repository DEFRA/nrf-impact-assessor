"""Spatial operations for assessments.

This module provides common spatial operations used across assessments:
- Clipping GeoDataFrames to extents
- Spatial joins with common predicates
- Geometry validation and repair
"""

import geopandas as gpd
from shapely import make_valid

from app.spatial.utils import apply_precision


def clip_gdf(
    gdf: gpd.GeoDataFrame,
    mask: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Clip a GeoDataFrame to the extent of a mask GeoDataFrame."""
    if gdf.crs != mask.crs:
        mask = mask.to_crs(gdf.crs)

    return gpd.clip(gdf, mask)


def spatial_join_intersect(
    left: gpd.GeoDataFrame,
    right: gpd.GeoDataFrame,
    grid_size: float = 0.0001,
) -> gpd.GeoDataFrame:
    """Spatial intersection (overlay) operation with precision control."""
    if left.crs != right.crs:
        right = right.to_crs(left.crs)

    left_precise = apply_precision(left, grid_size=grid_size)
    right_precise = apply_precision(right, grid_size=grid_size)

    result = gpd.overlay(
        left_precise, right_precise, how="intersection", keep_geom_type=False
    )

    return apply_precision(result, grid_size=grid_size)


def make_valid_geometries(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Repair invalid geometries using Shapely's make_valid."""
    gdf = gdf.copy()
    gdf["geometry"] = make_valid(gdf.geometry.values)
    return gdf
