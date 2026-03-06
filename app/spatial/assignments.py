"""Spatial assignments for assessments.

This module provides spatial assignment operations for assigning attributes
from overlay layers to input features based on spatial relationships.

Available operations:
- majority_overlap: Assign based on largest overlapping area (with parallel support)
- any_intersection: Assign all intersecting features as list
- nearest: Assign nearest feature
- intersection: Full spatial overlay for area calculations
"""

import logging
import os
from concurrent.futures import ProcessPoolExecutor
from typing import Any

import geopandas as gpd
import pandas as pd

logger = logging.getLogger(__name__)


def _majority_overlap_sequential(
    input_gdf: gpd.GeoDataFrame,
    overlay_gdf: gpd.GeoDataFrame,
    input_id_col: str,
    overlay_attr_col: str,
    output_field: str,
    default_value: Any | None = None,
) -> gpd.GeoDataFrame:
    """Assign overlay attribute based on majority spatial overlap (sequential).

    For each input feature, finds the overlay feature with the largest
    overlapping area and assigns its attribute value.

    Args:
        input_gdf: Input features (developments, sites, etc.)
        overlay_gdf: Overlay features (catchments, zones, etc.)
        input_id_col: ID column in input_gdf
        overlay_attr_col: Attribute column to assign from overlay_gdf
        output_field: Name of output field to create in input_gdf
        default_value: Value for features with no overlap

    Returns:
        input_gdf with new column containing assigned values

    Raises:
        ValueError: If required columns not found in GeoDataFrames
    """
    if input_id_col not in input_gdf.columns:
        msg = f"input_id_col '{input_id_col}' not found in input GeoDataFrame"
        raise ValueError(msg)
    if overlay_attr_col not in overlay_gdf.columns:
        msg = f"overlay_attr_col '{overlay_attr_col}' not found in overlay GeoDataFrame"
        raise ValueError(msg)

    # Drop 'id' column from overlay to avoid conflicts with input's 'id'
    # (the PostGIS tables have UUID 'id' primary keys)
    overlay_gdf_clean = overlay_gdf.drop(columns=["id"], errors="ignore")

    # Handle CRS mismatch
    if input_gdf.crs != overlay_gdf_clean.crs:
        overlay_gdf_clean = overlay_gdf_clean.to_crs(input_gdf.crs)

    # Perform spatial intersection
    intersections = gpd.overlay(input_gdf, overlay_gdf_clean, how="intersection")
    intersections["overlap_area"] = intersections.geometry.area

    # For each input ID, find the overlay attribute with the largest overlap
    majority = intersections.loc[
        intersections.groupby(input_id_col)["overlap_area"].idxmax(),
        [input_id_col, overlay_attr_col],
    ].reset_index(drop=True)

    # Create full result with all input IDs
    all_inputs = input_gdf[[input_id_col]].copy()
    assignments = all_inputs.merge(majority, on=input_id_col, how="left")

    # Fill missing values with default
    if default_value is not None:
        assignments[overlay_attr_col] = assignments[overlay_attr_col].fillna(
            default_value
        )

    # Rename to output field
    assignments = assignments.rename(columns={overlay_attr_col: output_field})

    # Merge back to input
    return input_gdf.merge(
        assignments[[input_id_col, output_field]], on=input_id_col, how="left"
    )


def _partition_by_bounds(
    gdf: gpd.GeoDataFrame, n_chunks: int
) -> list[gpd.GeoDataFrame]:
    """Partition GeoDataFrame into roughly equal spatial chunks.

    Splits along the longer axis (x or y) of the total bounds.
    """
    if len(gdf) == 0 or n_chunks <= 1:
        return [gdf]

    bounds = gdf.total_bounds  # minx, miny, maxx, maxy
    x_range = bounds[2] - bounds[0]
    y_range = bounds[3] - bounds[1]

    # Pre-compute centroids once (avoid recomputing on every loop iteration)
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


def _process_overlap_chunk(
    input_chunk: gpd.GeoDataFrame,
    overlay_gdf: gpd.GeoDataFrame,
    input_id_col: str,
    overlay_attr_col: str,
    output_field: str,
    default_value: Any,
) -> gpd.GeoDataFrame:
    """Process a single chunk for majority_overlap (runs in worker process)."""
    return _majority_overlap_sequential(
        input_chunk,
        overlay_gdf,
        input_id_col,
        overlay_attr_col,
        output_field,
        default_value,
    )


def majority_overlap(
    input_gdf: gpd.GeoDataFrame,
    overlay_gdf: gpd.GeoDataFrame,
    input_id_col: str,
    overlay_attr_col: str,
    output_field: str,
    default_value: Any | None = None,
    parallel: bool = True,
    max_workers: int | None = None,
) -> gpd.GeoDataFrame:
    """Assign overlay attribute based on majority spatial overlap.

    Supports parallel processing for large datasets.

    Args:
        input_gdf: Input features
        overlay_gdf: Overlay features
        input_id_col: ID column in input_gdf
        overlay_attr_col: Attribute column from overlay_gdf
        output_field: Name of output field
        default_value: Value for features with no overlap
        parallel: Enable parallel processing (default True)
        max_workers: Number of worker processes (default: 80% of cpu_count)
    """
    # Use sequential for small datasets or when disabled
    if not parallel or len(input_gdf) < 100:
        return _majority_overlap_sequential(
            input_gdf,
            overlay_gdf,
            input_id_col,
            overlay_attr_col,
            output_field,
            default_value,
        )

    if max_workers is None:
        # Cap at 80% of available CPUs to avoid saturating the host
        max_workers = max(1, int((os.cpu_count() or 4) * 0.8))

    # Partition input spatially
    chunks = _partition_by_bounds(input_gdf, max_workers)

    if len(chunks) <= 1:
        return _majority_overlap_sequential(
            input_gdf,
            overlay_gdf,
            input_id_col,
            overlay_attr_col,
            output_field,
            default_value,
        )

    logger.info(
        f"Processing {len(input_gdf)} features in {len(chunks)} parallel chunks"
    )

    try:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(
                    _process_overlap_chunk,
                    chunk,
                    overlay_gdf,
                    input_id_col,
                    overlay_attr_col,
                    output_field,
                    default_value,
                )
                for chunk in chunks
            ]
            results = [f.result() for f in futures]
    except (NotImplementedError, PermissionError, OSError) as exc:
        logger.warning(
            f"Parallel majority_overlap unavailable ({exc}); falling back to sequential"
        )
        return _majority_overlap_sequential(
            input_gdf,
            overlay_gdf,
            input_id_col,
            overlay_attr_col,
            output_field,
            default_value,
        )

    # Combine results
    combined = pd.concat(results, ignore_index=True)

    # Restore original geometry and order
    return input_gdf.merge(
        combined[[input_id_col, output_field]], on=input_id_col, how="left"
    )


def any_intersection(
    input_gdf: gpd.GeoDataFrame,
    overlay_gdf: gpd.GeoDataFrame,
    input_id_col: str,
    overlay_attr_col: str,
    output_field: str,
) -> gpd.GeoDataFrame:
    """Assign all intersecting overlay attributes as a list.

    For each input feature, collects all overlay features that intersect it
    and assigns their attribute values as a list.

    Args:
        input_gdf: Input features
        overlay_gdf: Overlay features
        input_id_col: ID column in input_gdf
        overlay_attr_col: Attribute column to assign from overlay_gdf
        output_field: Name of output field to create in input_gdf

    Returns:
        input_gdf with new column containing list of assigned values
    """
    if input_gdf.crs != overlay_gdf.crs:
        overlay_gdf = overlay_gdf.to_crs(input_gdf.crs)

    # Perform spatial join to find all intersections
    intersections = gpd.sjoin(
        input_gdf, overlay_gdf, how="left", predicate="intersects"
    )

    # Group by input ID and collect all overlay attributes
    if overlay_attr_col in intersections.columns:
        assignments = (
            intersections.groupby(input_id_col)[overlay_attr_col]
            .apply(lambda x: list(x.dropna().unique()))
            .reset_index()
            .rename(columns={overlay_attr_col: output_field})
        )
    else:
        # No intersections found
        assignments = pd.DataFrame(
            {input_id_col: input_gdf[input_id_col], output_field: [[]] * len(input_gdf)}
        )

    # Merge back to input
    result = input_gdf.merge(assignments, on=input_id_col, how="left")

    # Fill any NaN with empty lists
    result[output_field] = result[output_field].apply(
        lambda x: x if isinstance(x, list) else []
    )

    return result


def nearest(
    input_gdf: gpd.GeoDataFrame,
    overlay_gdf: gpd.GeoDataFrame,
    input_id_col: str,
    overlay_attr_col: str,
    output_field: str,
    max_distance: float | None = None,
) -> gpd.GeoDataFrame:
    """Assign nearest overlay feature attribute.

    For each input feature, finds the nearest overlay feature and assigns
    its attribute value.

    Args:
        input_gdf: Input features
        overlay_gdf: Overlay features
        input_id_col: ID column in input_gdf
        overlay_attr_col: Attribute column to assign from overlay_gdf
        output_field: Name of output field to create in input_gdf
        max_distance: Maximum distance to search (None = unlimited)

    Returns:
        input_gdf with new column containing assigned values
    """
    if input_gdf.crs != overlay_gdf.crs:
        overlay_gdf = overlay_gdf.to_crs(input_gdf.crs)

    # Use sjoin_nearest (available in GeoPandas >= 0.10)
    nearest_features = gpd.sjoin_nearest(
        input_gdf[[input_id_col, "geometry"]],
        overlay_gdf[[overlay_attr_col, "geometry"]],
        how="left",
        max_distance=max_distance,
        distance_col="distance",
    )

    # Keep only the first match per input feature (nearest)
    nearest_features = nearest_features.groupby(input_id_col).first().reset_index()

    # Rename to output field
    assignments = nearest_features[[input_id_col, overlay_attr_col]].rename(
        columns={overlay_attr_col: output_field}
    )

    # Merge back to input
    return input_gdf.merge(assignments, on=input_id_col, how="left")


def intersection(
    input_gdf: gpd.GeoDataFrame,
    overlay_gdf: gpd.GeoDataFrame,
    _preserve_input_fields: bool = True,  # Reserved for future field filtering
) -> gpd.GeoDataFrame:
    """Perform spatial intersection overlay.

    Unlike assignment operations that add a column, this returns the full
    intersection overlay result with geometries split at boundaries.

    This is useful when you need the actual intersection geometries
    (e.g., for calculating areas of overlap).

    Args:
        input_gdf: Input features
        overlay_gdf: Overlay features
        preserve_input_fields: If True, preserve all input fields in result

    Returns:
        Intersection overlay result (may have more rows than input)
    """
    if input_gdf.crs != overlay_gdf.crs:
        overlay_gdf = overlay_gdf.to_crs(input_gdf.crs)

    return gpd.overlay(input_gdf, overlay_gdf, how="intersection", keep_geom_type=False)


def execute_assignment(
    input_gdf: gpd.GeoDataFrame,
    overlay_gdf: gpd.GeoDataFrame,
    strategy: str,
    input_id_col: str,
    overlay_attr_col: str,
    output_field: str,
    **kwargs,
) -> gpd.GeoDataFrame:
    """Execute a spatial assignment operation.

    This is the main entry point for the runner to perform assignments.

    Args:
        input_gdf: Input features
        overlay_gdf: Overlay features
        strategy: Strategy name ('majority_overlap', 'any_intersection', 'nearest', 'intersection')
        input_id_col: ID column in input_gdf
        overlay_attr_col: Attribute column from overlay_gdf
        output_field: Name of output field
        **kwargs: Additional strategy-specific parameters

    Returns:
        input_gdf with assignment results

    Raises:
        ValueError: If strategy is unknown
    """
    if strategy == "majority_overlap":
        return majority_overlap(
            input_gdf,
            overlay_gdf,
            input_id_col,
            overlay_attr_col,
            output_field,
            **kwargs,
        )
    if strategy == "any_intersection":
        return any_intersection(
            input_gdf, overlay_gdf, input_id_col, overlay_attr_col, output_field
        )
    if strategy == "nearest":
        return nearest(
            input_gdf,
            overlay_gdf,
            input_id_col,
            overlay_attr_col,
            output_field,
            **kwargs,
        )
    if strategy == "intersection":
        return intersection(input_gdf, overlay_gdf, **kwargs)
    msg = (
        f"Unknown assignment strategy: {strategy}. "
        f"Supported: majority_overlap, any_intersection, nearest, intersection"
    )
    raise ValueError(msg)
