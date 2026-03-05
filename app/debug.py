"""Debug output helpers for spatial monitoring.

WARNING: For local development and debugging only. Never enable in production.
"""

import logging
from datetime import UTC, datetime

import geopandas as gpd

from app.config import DebugConfig

logger = logging.getLogger(__name__)


def save_debug_gdf(
    gdf: gpd.GeoDataFrame,
    name: str,
    job_id: str,
    config: DebugConfig,
) -> None:
    """Save GeoDataFrame for debugging if debug output is enabled.

    Args:
        gdf: GeoDataFrame to save
        name: Descriptive name (e.g., "coefficient_filtered", "land_use_intersections")
        job_id: Job identifier for organizing output
        config: Debug configuration
    """
    if not config.enabled:
        return

    output_dir = config.output_dir / job_id
    output_dir.mkdir(mode=0o700, parents=True, exist_ok=True)

    timestamp = datetime.now(UTC).strftime("%H%M%S")
    filename = f"{timestamp}_{name}.gpkg"
    output_path = output_dir / filename

    try:
        gdf.to_file(output_path, driver="GPKG")
        logger.debug(f"Saved debug output: {output_path} ({len(gdf)} features)")
    except Exception as e:
        logger.warning(f"Failed to save debug output {name}: {e}")
