"""General spatial utilities.

This module provides common spatial utilities used across assessments:
- CRS validation and transformation
- Precision model application (for ArcGIS compatibility)
"""

import geopandas as gpd
from shapely import set_precision


def ensure_crs(
    gdf: gpd.GeoDataFrame, target_crs: str = "EPSG:27700"
) -> gpd.GeoDataFrame:
    """Ensure GeoDataFrame is in the target CRS, transforming if necessary."""
    if gdf.crs is None:
        msg = "Input GeoDataFrame has no CRS defined"
        raise ValueError(msg)

    if gdf.crs != target_crs:
        return gdf.to_crs(target_crs)

    return gdf


def apply_precision(
    gdf: gpd.GeoDataFrame,
    grid_size: float = 0.0001,
) -> gpd.GeoDataFrame:
    """Apply precision model to geometries to match ArcGIS XY tolerance behavior."""
    gdf = gdf.copy()
    gdf["geometry"] = set_precision(gdf.geometry.values, grid_size=grid_size)
    return gdf
