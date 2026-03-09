"""Spatial operations for impact assessments.

This package provides spatial operations including:
- Assignment operations (majority overlap, nearest, intersection, etc.)
- Overlay operations (buffer, difference with precision control)
- Common operations (clip, spatial join with precision, geometry validation)
- General utilities (CRS handling, precision models)

Commonly used exports:
- execute_assignment: Main entry point for spatial assignments
- majority_overlap: Assign based on largest overlapping area
- any_intersection: Assign all intersecting features
- nearest: Assign nearest feature
- intersection: Full spatial overlay
- buffer_with_dissolve: Buffer with optional dissolve
- spatial_join_intersect: Spatial intersection with precision control (replaces PairwiseIntersect)
- spatial_difference_with_precision: Difference (erase) with precision control
- clip_gdf: Clip GeoDataFrame to extent
- make_valid_geometries: Repair invalid geometries
- ensure_crs: CRS validation and transformation
- apply_precision: Apply precision model to geometries
"""

# Assignment operations
from app.spatial.assignments import (
    any_intersection,
    execute_assignment,
    intersection,
    majority_overlap,
    nearest,
)

# Common operations
from app.spatial.operations import (
    clip_gdf,
    make_valid_geometries,
    spatial_join_intersect,
)

# Overlay operations
from app.spatial.overlay import (
    buffer_with_dissolve,
    spatial_difference_with_precision,
)

# General utilities
from app.spatial.utils import apply_precision, ensure_crs

__all__ = [
    "execute_assignment",
    "majority_overlap",
    "any_intersection",
    "nearest",
    "intersection",
    "buffer_with_dissolve",
    "spatial_difference_with_precision",
    "clip_gdf",
    "spatial_join_intersect",
    "make_valid_geometries",
    "ensure_crs",
    "apply_precision",
]
