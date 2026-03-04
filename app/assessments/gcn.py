"""GCN (Great crested newt) impact assessment.

This module implements the GCN assessment following the simplified pluggable
architecture. It owns all GCN domain logic while the platform provides data
access through the repository.
"""

import logging

import geopandas as gpd
import pandas as pd
from geoalchemy2.functions import ST_GeomFromText, ST_Intersects, ST_SetSRID
from shapely.ops import unary_union
from sqlalchemy import select

from app.config import DEFAULT_GCN_CONFIG, GcnConfig
from app.models.db import SpatialLayer
from app.models.enums import SpatialLayerType
from app.repositories.repository import Repository
from app.spatial.operations import (
    clip_gdf,
    make_valid_geometries,
    spatial_join_intersect,
)
from app.spatial.overlay import buffer_with_dissolve
from app.spatial.utils import ensure_crs

logger = logging.getLogger(__name__)


class GcnAssessment:
    """GCN (Great Crested Newt) impact assessment."""

    def __init__(
        self,
        rlb_gdf: gpd.GeoDataFrame,
        metadata: dict,
        repository: Repository,
        config: GcnConfig = DEFAULT_GCN_CONFIG,
    ):
        self.rlb_gdf = rlb_gdf
        self.metadata = metadata
        self.repository = repository
        self.config = config

    def run(self) -> dict[str, pd.DataFrame]:
        """Run GCN impact assessment."""
        unique_ref = self.metadata["unique_ref"]
        survey_ponds_path = self.metadata.get("survey_ponds_path")
        target_srid = _crs_to_srid(self.config.target_crs)

        logger.info(f"Running GCN assessment: {unique_ref}")

        rlb = ensure_crs(self.rlb_gdf, target_crs=self.config.target_crs)
        rlb = make_valid_geometries(rlb)
        rlb["UniqueRef"] = unique_ref

        logger.info(f"Creating {self.config.buffer_distance_m}m buffer")
        rlb_buffered = buffer_with_dissolve(
            rlb,
            self.config.buffer_distance_m,
            dissolve=True,
            grid_size=self.config.precision_grid_size,
        )
        rlb_buffered["Area"] = "Buffer"

        rlb["Area"] = "RLB"
        rlb_with_buffer = pd.concat([rlb, rlb_buffered], ignore_index=True)

        combined_geom = unary_union(rlb_with_buffer.geometry)
        combined_extent = gpd.GeoDataFrame(
            {"geometry": [combined_geom]}, crs=rlb_with_buffer.crs
        )
        filter_wkt = combined_geom.wkt

        logger.info("Loading risk zones from repository (server-side clipped)")
        risk_zones_clipped = self.repository.intersection_postgis(
            input_gdf=combined_extent,
            overlay_table=SpatialLayer,
            overlay_filter=(SpatialLayer.layer_type == SpatialLayerType.GCN_RISK_ZONES),
            overlay_columns=["attributes"],
        )

        if "attributes" in risk_zones_clipped.columns:
            risk_zones_clipped["RZ"] = risk_zones_clipped["attributes"].apply(
                lambda x: x.get("RZ") if x else None
            )
        if "RZ" not in risk_zones_clipped.columns:
            msg = "Risk zones missing required 'RZ' attribute"
            raise ValueError(msg)
        if risk_zones_clipped["RZ"].isna().all():
            msg = "Risk zones missing required 'RZ' values"
            raise ValueError(msg)
        logger.info(f"Loaded {len(risk_zones_clipped)} risk zone features")

        if survey_ponds_path:
            logger.info(f"Loading survey ponds: {survey_ponds_path}")
            ponds = gpd.read_file(survey_ponds_path)
            ponds = ensure_crs(ponds, target_crs=self.config.target_crs)
            if "PANS" not in ponds.columns:
                msg = "Survey ponds must have 'PANS' column"
                raise ValueError(msg)
            if "TmpImp" not in ponds.columns:
                logger.warning(
                    "Survey ponds missing 'TmpImp' column, defaulting to 'F'"
                )
                ponds["TmpImp"] = "F"
        else:
            logger.info("Loading national ponds with spatial filtering")
            stmt = select(SpatialLayer).where(
                SpatialLayer.layer_type == SpatialLayerType.GCN_PONDS,
                ST_Intersects(
                    SpatialLayer.geometry,
                    ST_SetSRID(ST_GeomFromText(filter_wkt), target_srid),
                ),
            )
            ponds = self.repository.execute_query(stmt, as_gdf=True)
            ponds["PANS"] = "NS"
            ponds["TmpImp"] = "F"
            logger.info(f"Loaded {len(ponds)} ponds within RLB+buffer extent")

        logger.info("Assigning ponds to RLB and Buffer areas")
        all_ponds_clipped = clip_gdf(ponds, combined_extent[["geometry"]])

        rlb_intersecting_ponds = gpd.sjoin(
            all_ponds_clipped, rlb[["geometry"]], predicate="intersects", how="inner"
        )
        rlb_pond_indices = rlb_intersecting_ponds.index.unique()
        ponds_in_rlb = all_ponds_clipped.loc[rlb_pond_indices].copy()
        ponds_in_rlb = clip_gdf(ponds_in_rlb, rlb[["geometry"]])
        ponds_in_rlb["Area"] = "RLB"

        ponds_in_buffer = all_ponds_clipped[
            ~all_ponds_clipped.index.isin(rlb_intersecting_ponds.index)
        ].copy()
        ponds_in_buffer["Area"] = "Buffer"

        all_ponds = pd.concat([ponds_in_rlb, ponds_in_buffer], ignore_index=True)
        logger.info(
            f"Found {len(ponds_in_rlb)} ponds in RLB, {len(ponds_in_buffer)} in buffer"
        )

        logger.info("Calculating habitat impact")
        habitat_impact = _calculate_habitat_impact(
            rlb_with_buffer,
            risk_zones_clipped,
            all_ponds,
            pond_buffer_distance_m=self.config.pond_buffer_distance_m,
            precision_grid_size=self.config.precision_grid_size,
        )

        logger.info("Calculating pond frequency")
        pond_frequency = _calculate_pond_frequency(
            ponds_in_rlb, ponds_in_buffer, risk_zones_clipped
        )

        logger.info("GCN assessment complete")

        return {"habitat_impact": habitat_impact, "pond_frequency": pond_frequency}


def _calculate_habitat_impact(
    rlb_with_buffer: gpd.GeoDataFrame,
    risk_zones: gpd.GeoDataFrame,
    ponds: gpd.GeoDataFrame,
    pond_buffer_distance_m: int = 250,
    precision_grid_size: float = 0.0001,
) -> pd.DataFrame:
    """Calculate habitat impact by risk zone."""
    ponds_buffered = buffer_with_dissolve(
        ponds,
        pond_buffer_distance_m,
        dissolve=True,
        grid_size=precision_grid_size,
    )

    risk_zones_in_pond_buffer = clip_gdf(risk_zones, ponds_buffered)

    habitat_impact = spatial_join_intersect(
        rlb_with_buffer,
        risk_zones_in_pond_buffer,
        grid_size=precision_grid_size,
    )

    habitat_impact["Shape_Area"] = habitat_impact.geometry.area

    habitat_impact = habitat_impact[habitat_impact["Shape_Area"] > 0].copy()

    return habitat_impact[["Area", "RZ", "Shape_Area"]].copy()


def _calculate_pond_frequency(
    ponds_in_rlb: gpd.GeoDataFrame,
    ponds_in_buffer: gpd.GeoDataFrame,
    risk_zones: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """Calculate pond frequency by zone and status."""
    ponds_in_rlb = ponds_in_rlb.copy()
    ponds_in_buffer = ponds_in_buffer.copy()

    ponds_in_rlb["Pond_ID"] = ["RLB_" + str(i) for i in range(len(ponds_in_rlb))]
    ponds_in_buffer["Pond_ID"] = ["BUF_" + str(i) for i in range(len(ponds_in_buffer))]

    all_ponds = pd.concat([ponds_in_rlb, ponds_in_buffer], ignore_index=True)

    ponds_with_zones = gpd.sjoin(
        all_ponds,
        risk_zones[["geometry", "RZ"]],
        how="inner",
        predicate="intersects",
    )

    pond_zones = (
        ponds_with_zones[["Pond_ID", "PANS", "TmpImp", "Area", "RZ"]]
        .groupby(["Pond_ID", "PANS", "TmpImp", "Area"])["RZ"]
        .apply(lambda x: ":".join(sorted(set(x))))
        .reset_index()
        .rename(columns={"RZ": "CONCATENATE_RZ"})
    )

    def highest_zone(zones: str) -> str:
        zone_list = zones.split(":")
        if "Red" in zone_list:
            return "Red"
        if "Amber" in zone_list:
            return "Amber"
        if "Green" in zone_list:
            return "Green"
        return zone_list[0] if zone_list else "Unknown"

    pond_zones["MaxZone"] = pond_zones["CONCATENATE_RZ"].apply(highest_zone)

    return (
        pond_zones.groupby(["PANS", "Area", "MaxZone", "TmpImp"])
        .size()
        .reset_index(name="FREQUENCY")
    )


def _crs_to_srid(crs: str) -> int:
    """Extract SRID integer from EPSG CRS string."""
    if not crs.startswith("EPSG:"):
        msg = f"Unsupported CRS format: {crs}. Expected 'EPSG:<srid>'."
        raise ValueError(msg)
    return int(crs.split(":", maxsplit=1)[1])
