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

# Standard valid range for longitude/latitude.
_WGS84_LON_RANGE = (-180.0, 180.0)
_WGS84_LAT_RANGE = (-90.0, 90.0)

# Generous eastings/northings bounds covering all of England (Isles of Scilly
# to the Scottish border) with margin. BNG has no natural mathematical limit
# the way lon/lat does, so a garbled or unprojected value (e.g. off by
# several orders of magnitude) would otherwise pass geometry validation
# unnoticed and only surface later, when reprojecting to WGS84 for the
# response overflows to `inf` and crashes JSON serialisation.
_BNG_EASTING_RANGE = (0.0, 700_000.0)
_BNG_NORTHING_RANGE = (0.0, 700_000.0)


def validate_coordinate_range(gdf: gpd.GeoDataFrame) -> str | None:
    """Validate that coordinates fall within sensible bounds for their CRS.

    Must run before ensure_crs()/any reprojection: reprojecting an
    out-of-domain BNG easting/northing (or a nonsensical WGS84 lon/lat) can
    overflow to `inf`, which json.dumps refuses to serialise.

    Returns:
        Failure code string if any coordinate is out of range, or None.
    """
    if gdf.crs is None:
        return None

    epsg = gdf.crs.to_epsg()
    if epsg == 4326:
        x_range, y_range = _WGS84_LON_RANGE, _WGS84_LAT_RANGE
    elif epsg == 27700:
        x_range, y_range = _BNG_EASTING_RANGE, _BNG_NORTHING_RANGE
    else:
        return None

    min_x, min_y, max_x, max_y = gdf.total_bounds
    if (
        min_x < x_range[0]
        or max_x > x_range[1]
        or min_y < y_range[0]
        or max_y > y_range[1]
    ):
        return "coordinates_out_of_range"

    return None


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
