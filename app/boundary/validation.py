"""Geometry validation for uploaded boundary files.

Validates that uploaded Red Line Boundary geometries form valid,
non-intersecting polygons suitable for assessment.
"""

import geopandas as gpd
import numpy as np

_VALID_GEOM_TYPES = {"Polygon"}

SUPPORTED_CRS = {
    27700: "British National Grid",
    4326: "WGS 84",
}


def _has_duplicate_consecutive_vertices(geom) -> bool:
    """Check whether a polygon has duplicate consecutive vertices."""
    coords = np.array(geom.exterior.coords)
    diffs = np.diff(coords, axis=0)
    zero_mask = np.all(diffs == 0, axis=1)
    return bool(zero_mask.any())


def _has_holes(geom) -> bool:
    """Check whether a polygon contains interior rings (holes)."""
    return len(list(geom.interiors)) > 0


def validate_geometry(gdf: gpd.GeoDataFrame) -> str | None:
    """Validate geometry data, returning a failure code on invalid input.

    Checks for unsupported geometry types, null geometries,
    invalid geometries (e.g. self-intersections), interior holes,
    and duplicate consecutive vertices.

    Returns:
        Failure code string if validation fails, or None if valid.
    """
    null_count = gdf.geometry.isna().sum()
    if null_count > 0:
        return "invalid_geometry"

    geom_types = set(gdf.geometry.geom_type.unique())
    invalid_types = geom_types - _VALID_GEOM_TYPES
    if invalid_types:
        return "unsupported_geometry_type"

    invalid_count = (~gdf.geometry.is_valid).sum()
    if invalid_count > 0:
        return "self_intersecting_geometry"

    for geom in gdf.geometry:
        if _has_holes(geom):
            return "geometry_has_holes"

    for geom in gdf.geometry:
        if _has_duplicate_consecutive_vertices(geom):
            return "duplicate_vertices"

    return None
