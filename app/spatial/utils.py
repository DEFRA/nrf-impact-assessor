"""General spatial utilities.

This module provides common spatial utilities used across assessments:
- CRS validation and transformation
- Precision model application (for ArcGIS compatibility)
"""

import geopandas as gpd
from pyproj import CRS
from pyproj.exceptions import CRSError
from shapely import set_precision

_SUPPORTED_EPSG_CODES = {27700, 4326}


class UnsupportedCRSError(ValueError):
    """Raised when the input CRS is not in the supported set."""


def ensure_crs(
    gdf: gpd.GeoDataFrame, target_crs: str = "EPSG:27700"
) -> gpd.GeoDataFrame:
    """Ensure GeoDataFrame is in the target CRS, transforming if necessary."""
    if gdf.crs is None:
        msg = "Input GeoDataFrame has no CRS defined"
        raise ValueError(msg)

    try:
        epsg = CRS(gdf.crs).to_epsg()
    except CRSError as e:
        msg = f"Unrecognised coordinate reference system: {e}"
        raise UnsupportedCRSError(msg) from e

    if epsg not in _SUPPORTED_EPSG_CODES:
        msg = f"Unsupported coordinate reference system: EPSG:{epsg}"
        raise UnsupportedCRSError(msg)

    if gdf.crs != target_crs:
        return gdf.to_crs(target_crs)

    return gdf


def apply_precision(
    gdf: gpd.GeoDataFrame,
    grid_size: float = 0.0001,
) -> gpd.GeoDataFrame:
    """Apply precision model to geometries to match ArcGIS XY tolerance behavior.

    Uses set_geometry() rather than a full DataFrame copy so only the geometry
    array is replaced; all other column data is shared with the original.
    """
    return gdf.set_geometry(set_precision(gdf.geometry.values, grid_size=grid_size))
