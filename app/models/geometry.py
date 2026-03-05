"""Geometry-related domain models."""

from enum import StrEnum


class GeometryFormat(StrEnum):
    """Supported geometry file formats."""

    SHAPEFILE = "shapefile"
    GEOJSON = "geojson"
