"""Spatial overlay operations for assessments.

This module provides spatial overlay operations commonly used in assessments,
particularly for GCN (Great Crested Newt) assessments that require:
- Buffer operations with dissolve
- Difference (erase) with precision control
"""

import logging
import os
from concurrent.futures import ProcessPoolExecutor

import geopandas as gpd
import pandas as pd
from shapely.ops import unary_union

from app.spatial.utils import apply_precision

logger = logging.getLogger(__name__)


def buffer_with_dissolve(
    gdf: gpd.GeoDataFrame,
    distance_m: float,
    dissolve: bool = True,
    grid_size: float = 0.0001,
) -> gpd.GeoDataFrame:
    """Buffer geometries with optional dissolve to single geometry."""

    buffered = gdf.copy()
    buffered["geometry"] = buffered.geometry.buffer(distance_m)

    if dissolve:
        dissolved_geom = unary_union(buffered.geometry)
        buffered = gpd.GeoDataFrame({"geometry": [dissolved_geom]}, crs=buffered.crs)

    return apply_precision(buffered, grid_size=grid_size)


def spatial_difference_with_precision(
    left: gpd.GeoDataFrame,
    right: gpd.GeoDataFrame,
    grid_size: float = 0.0001,
    parallel: bool = True,
    max_workers: int | None = None,
) -> gpd.GeoDataFrame:
    """Spatial difference (erase) with precision model applied."""
    if left.crs != right.crs:
        right = right.to_crs(left.crs)

    left_precise = apply_precision(left, grid_size=grid_size)
    right_precise = apply_precision(right, grid_size=grid_size)

    if not parallel or len(left_precise) < 100:
        result = gpd.overlay(
            left_precise, right_precise, how="difference", keep_geom_type=False
        )
        return apply_precision(result, grid_size=grid_size)

    if max_workers is None:
        max_workers = max(1, int((os.cpu_count() or 4) * 0.8))

    chunks = _partition_by_bounds(left_precise, max_workers)
    if len(chunks) <= 1:
        result = gpd.overlay(
            left_precise, right_precise, how="difference", keep_geom_type=False
        )
        return apply_precision(result, grid_size=grid_size)

    try:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(_difference_chunk, chunk, right_precise)
                for chunk in chunks
            ]
            results = [f.result() for f in futures]
    except (NotImplementedError, PermissionError, OSError) as exc:
        logger.warning(
            f"Parallel spatial_difference unavailable ({exc}); falling back to sequential"
        )
        result = gpd.overlay(
            left_precise, right_precise, how="difference", keep_geom_type=False
        )
        return apply_precision(result, grid_size=grid_size)

    result = gpd.GeoDataFrame(
        pd.concat(results, ignore_index=True),
        crs=left_precise.crs,
    )

    return apply_precision(result, grid_size=grid_size)


def _difference_chunk(
    left_chunk: gpd.GeoDataFrame,
    right_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Run a difference overlay for one left-side chunk."""
    return gpd.overlay(left_chunk, right_gdf, how="difference", keep_geom_type=False)


def _partition_by_bounds(
    gdf: gpd.GeoDataFrame, n_chunks: int
) -> list[gpd.GeoDataFrame]:
    """Partition GeoDataFrame into non-overlapping spatial chunks."""
    if len(gdf) == 0 or n_chunks <= 1:
        return [gdf]

    bounds = gdf.total_bounds  # minx, miny, maxx, maxy
    x_range = bounds[2] - bounds[0]
    y_range = bounds[3] - bounds[1]

    centroids = gdf.geometry.centroid
    cx = centroids.x
    cy = centroids.y

    chunks = []
    if x_range >= y_range:
        step = x_range / n_chunks
        for i in range(n_chunks):
            min_x = bounds[0] + i * step
            max_x = bounds[0] + (i + 1) * step
            if i < n_chunks - 1:
                chunk = gdf[(cx >= min_x) & (cx < max_x)]
            else:
                chunk = gdf[(cx >= min_x) & (cx <= bounds[2])]
            if len(chunk) > 0:
                chunks.append(chunk)
    else:
        step = y_range / n_chunks
        for i in range(n_chunks):
            min_y = bounds[1] + i * step
            max_y = bounds[1] + (i + 1) * step
            if i < n_chunks - 1:
                chunk = gdf[(cy >= min_y) & (cy < max_y)]
            else:
                chunk = gdf[(cy >= min_y) & (cy <= bounds[3])]
            if len(chunk) > 0:
                chunks.append(chunk)

    return chunks if chunks else [gdf]
