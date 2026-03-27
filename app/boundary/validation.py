"""Geometry validation for uploaded boundary files.

Validates that uploaded Red Line Boundary geometries form valid,
non-intersecting polygons suitable for assessment.
"""

import geopandas as gpd
import numpy as np

_VALID_GEOM_TYPES = {"Polygon"}

_SUPPORTED_CRS_LABELS = [
    "EPSG:27700 (British National Grid)",
    "EPSG:4326 (WGS 84)",
]


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
    """Validate geometry data, returning an error message on invalid input.

    Checks for unsupported geometry types, null geometries,
    invalid geometries (e.g. self-intersections), interior holes,
    and duplicate consecutive vertices.

    Returns:
        Error message string if validation fails, or None if valid.
    """
    geom_types = set(gdf.geometry.geom_type.unique())
    invalid_types = geom_types - _VALID_GEOM_TYPES
    if invalid_types:
        return (
            f"Invalid geometry types found: {', '.join(invalid_types)}. "
            "Only Polygon geometry is supported. "
            "Please ensure the boundary forms a complete closed polygon shape."
        )

    null_count = gdf.geometry.isna().sum()
    if null_count > 0:
        return (
            "The uploaded boundary geometry could not be processed. "
            "The file contains incomplete or malformed coordinates. "
            "Please check your file and re-export the boundary from your GIS software."
        )

    invalid_count = (~gdf.geometry.is_valid).sum()
    if invalid_count > 0:
        return (
            "The uploaded boundary contains invalid geometry "
            "(self-intersecting or crossing line segments). "
            "Please correct the boundary so that edges do not cross each other "
            "and try again."
        )

    for geom in gdf.geometry:
        if _has_holes(geom):
            return (
                "The uploaded boundary contains interior holes or gaps. "
                "The boundary must be a single continuous area without holes. "
                "Please remove any interior rings and upload a simple polygon."
            )

    for geom in gdf.geometry:
        if _has_duplicate_consecutive_vertices(geom):
            return (
                "The uploaded boundary contains duplicated or overlapping geometry "
                "(duplicate consecutive vertices). "
                "Please clean up the boundary to remove "
                "duplicate points and try again."
            )

    return None
