"""Nutrient impact assessment.

This module implements the nutrient impact assessment following the simplified
pluggable architecture. It owns all nutrient domain logic while the platform
provides data access through the repository.
"""

import logging
import time

import geopandas as gpd
import numpy as np
import pandas as pd
from sqlalchemy import func, select

from app.calculators import (
    apply_buffer,
    apply_suds_mitigation,
    calculate_land_use_uplift,
    calculate_wastewater_load,
)
from app.config import CONSTANTS, AssessmentConfig, DebugConfig, RequiredColumns
from app.debug import save_debug_gdf
from app.models.db import CoefficientLayer, LookupTable, SpatialLayer
from app.models.enums import SpatialLayerType
from app.repositories.repository import Repository

logger = logging.getLogger(__name__)


class NutrientAssessment:
    """Nutrient impact assessment.

    This assessment evaluates nutrient impacts from proposed developments by:
    - Calculating land use change impacts within nutrient neutrality catchments
    - Calculating wastewater treatment impacts
    - Applying SuDS mitigation and precautionary buffers
    - Supporting both nitrogen and phosphorus nutrient pathways
    """

    def __init__(
        self,
        rlb_gdf: gpd.GeoDataFrame,
        metadata: dict,
        repository: Repository,
    ):
        self.rlb_gdf = rlb_gdf
        self.metadata = metadata
        self.repository = repository
        self.config = AssessmentConfig()
        self._debug_config = DebugConfig.from_env()
        self._version_cache: dict[str, int] = {}

    def run(self) -> dict[str, pd.DataFrame]:
        """Run nutrient impact assessment."""
        logger.info("Running nutrient impact assessment")
        t_total = time.perf_counter()

        t0 = time.perf_counter()
        rlb_gdf = self._validate_and_prepare_input(self.rlb_gdf)
        logger.info(
            f"[timing] validate_and_prepare_input: {time.perf_counter() - t0:.3f}s"
        )

        t0 = time.perf_counter()
        rlb_gdf = self._assign_spatial_features(rlb_gdf)
        logger.info(
            f"[timing] assign_spatial_features: {time.perf_counter() - t0:.3f}s"
        )

        t0 = time.perf_counter()
        rlb_gdf = self._calculate_land_use_impacts(rlb_gdf)
        logger.info(
            f"[timing] calculate_land_use_impacts: {time.perf_counter() - t0:.3f}s"
        )

        t0 = time.perf_counter()
        rlb_gdf = self._calculate_wastewater_impacts(rlb_gdf)
        logger.info(
            f"[timing] calculate_wastewater_impacts: {time.perf_counter() - t0:.3f}s"
        )

        t0 = time.perf_counter()
        rlb_gdf = self._calculate_totals(rlb_gdf)
        logger.info(f"[timing] calculate_totals: {time.perf_counter() - t0:.3f}s")

        t0 = time.perf_counter()
        rlb_gdf = self._filter_out_of_scope(rlb_gdf)
        logger.info(f"[timing] filter_out_of_scope: {time.perf_counter() - t0:.3f}s")

        save_debug_gdf(
            rlb_gdf, "99_final_rlb", self.metadata["unique_ref"], self._debug_config
        )

        logger.info(
            f"Nutrient assessment complete in {time.perf_counter() - t_total:.3f}s"
        )

        return {"impact_summary": rlb_gdf.drop(columns=["geometry"])}

    def _validate_and_prepare_input(
        self, rlb_gdf: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        """Validate and prepare input GeoDataFrame."""
        rlb_gdf = rlb_gdf.copy()

        input_column_map = {
            "Name": "name",
            "Dwel_Cat": "dwelling_category",
            "Source": "source",
            "Dwellings": "dwellings",
            "Shape_Area": "shape_area",
        }
        columns_to_rename = {
            k: v
            for k, v in input_column_map.items()
            if k in rlb_gdf.columns and v not in rlb_gdf.columns
        }
        if columns_to_rename:
            rlb_gdf = rlb_gdf.rename(columns=columns_to_rename)

        expected_cols = RequiredColumns.all()
        missing_cols = [col for col in expected_cols if col not in rlb_gdf.columns]
        if missing_cols:
            msg = (
                f"Required columns missing from input: {missing_cols}. "
                f"Expected columns: {expected_cols}"
            )
            raise ValueError(msg)

        if rlb_gdf.crs != CONSTANTS.CRS_BRITISH_NATIONAL_GRID:
            rlb_gdf = rlb_gdf.to_crs(CONSTANTS.CRS_BRITISH_NATIONAL_GRID)

        rlb_gdf[RequiredColumns.SHAPE_AREA] = rlb_gdf.geometry.area

        rlb_gdf = gpd.GeoDataFrame(rlb_gdf[expected_cols])

        rlb_gdf["rlb_id"] = range(1, len(rlb_gdf) + 1)

        rlb_gdf["dev_area_ha"] = (
            rlb_gdf[RequiredColumns.SHAPE_AREA] / CONSTANTS.SQUARE_METRES_PER_HECTARE
        ).round(2)

        return rlb_gdf

    def _resolve_latest_version(self, layer_type: SpatialLayerType) -> int:
        """Fetch the latest version number for a spatial layer type (cached)."""
        cache_key = f"spatial_{layer_type.name}"
        if cache_key not in self._version_cache:
            stmt = select(func.max(SpatialLayer.version)).where(
                SpatialLayer.layer_type == layer_type
            )
            result = self.repository.execute_query(stmt, as_gdf=False)
            self._version_cache[cache_key] = result[0] if result else 1
        return self._version_cache[cache_key]

    def _resolve_latest_coeff_version(self) -> int:
        """Fetch the latest version number for the coefficient layer (cached)."""
        cache_key = "coefficient"
        if cache_key not in self._version_cache:
            stmt = select(func.max(CoefficientLayer.version))
            result = self.repository.execute_query(stmt, as_gdf=False)
            self._version_cache[cache_key] = result[0] if result else 1
        return self._version_cache[cache_key]

    def _assign_spatial_features(self, rlb_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Assign spatial features via batched majority overlap."""
        logger.info("Assigning spatial features via batched PostGIS overlap")

        t0 = time.perf_counter()

        wwtw_ver = self._resolve_latest_version(SpatialLayerType.WWTW_CATCHMENTS)
        lpa_ver = self._resolve_latest_version(SpatialLayerType.LPA_BOUNDARIES)
        sub_ver = self._resolve_latest_version(SpatialLayerType.SUBCATCHMENTS)

        batch_results = self.repository.batch_majority_overlap_postgis(
            input_gdf=rlb_gdf,
            input_id_col="rlb_id",
            assignments=[
                {
                    "overlay_table": SpatialLayer,
                    "overlay_filter": (
                        (SpatialLayer.layer_type == SpatialLayerType.WWTW_CATCHMENTS)
                        & (SpatialLayer.version == wwtw_ver)
                    ),
                    "overlay_attr_col": SpatialLayer.attributes["WwTw_ID"].astext,
                    "output_field": "majority_wwtw_id",
                    "default_value": self.config.fallback_wwtw_id,
                },
                {
                    "overlay_table": SpatialLayer,
                    "overlay_filter": (
                        (SpatialLayer.layer_type == SpatialLayerType.LPA_BOUNDARIES)
                        & (SpatialLayer.version == lpa_ver)
                    ),
                    "overlay_attr_col": SpatialLayer.attributes["NAME"].astext,
                    "output_field": "majority_name",
                    "default_value": "UNKNOWN",
                },
                {
                    "overlay_table": SpatialLayer,
                    "overlay_filter": (
                        (SpatialLayer.layer_type == SpatialLayerType.SUBCATCHMENTS)
                        & (SpatialLayer.version == sub_ver)
                    ),
                    "overlay_attr_col": SpatialLayer.attributes["OPCAT_NAME"].astext,
                    "output_field": "majority_opcat_name",
                    "default_value": None,
                },
            ],
        )

        rlb_gdf = rlb_gdf.merge(
            batch_results["majority_wwtw_id"], on="rlb_id", how="left"
        )
        rlb_gdf["majority_wwtw_id"] = (
            pd.to_numeric(rlb_gdf["majority_wwtw_id"], errors="coerce")
            .fillna(self.config.fallback_wwtw_id)
            .astype(int)
        )
        save_debug_gdf(
            rlb_gdf,
            "04_after_wwtw_assignment",
            self.metadata["unique_ref"],
            self._debug_config,
        )

        rlb_gdf = rlb_gdf.merge(batch_results["majority_name"], on="rlb_id", how="left")
        save_debug_gdf(
            rlb_gdf,
            "05_after_lpa_assignment",
            self.metadata["unique_ref"],
            self._debug_config,
        )

        rlb_gdf = rlb_gdf.merge(
            batch_results["majority_opcat_name"], on="rlb_id", how="left"
        )
        save_debug_gdf(
            rlb_gdf,
            "06_after_subcatchment_assignment",
            self.metadata["unique_ref"],
            self._debug_config,
        )

        elapsed = time.perf_counter() - t0
        logger.info(
            f"[timing] spatial: batched PostGIS majority_overlap (3 layers): {elapsed:.3f}s"
        )

        return rlb_gdf

    def _calculate_land_use_impacts(
        self, rlb_gdf: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        """Calculate land use change nutrient impacts."""
        logger.info("Calculating land use impacts")

        nn_version = self._resolve_latest_version(SpatialLayerType.NN_CATCHMENTS)
        coeff_version = self._resolve_latest_coeff_version()

        land_use_intersections = self.repository.land_use_intersection_postgis(
            input_gdf=rlb_gdf,
            coeff_version=coeff_version,
            nn_version=nn_version,
        )
        logger.info(
            f"PostGIS land use intersection returned {len(land_use_intersections):,} rows"
        )

        if len(land_use_intersections) == 0:
            logger.info("No 3-way intersections found - no land use impacts")
            rlb_gdf["area_in_nn_catchment_ha"] = np.nan
            rlb_gdf["n_lu_uplift"] = np.nan
            rlb_gdf["p_lu_uplift"] = np.nan
            rlb_gdf["nn_catchment"] = None
            rlb_gdf["n_lu_post_suds"] = 0.0
            rlb_gdf["p_lu_post_suds"] = 0.0
            return rlb_gdf

        land_use_intersections["n_resi_coeff"] = pd.to_numeric(
            land_use_intersections["n_resi_coeff"], errors="coerce"
        )
        land_use_intersections["lu_curr_n_coeff"] = pd.to_numeric(
            land_use_intersections["lu_curr_n_coeff"], errors="coerce"
        )
        land_use_intersections["p_resi_coeff"] = pd.to_numeric(
            land_use_intersections["p_resi_coeff"], errors="coerce"
        )
        land_use_intersections["lu_curr_p_coeff"] = pd.to_numeric(
            land_use_intersections["lu_curr_p_coeff"], errors="coerce"
        )

        land_use_intersections = land_use_intersections.merge(
            rlb_gdf[["rlb_id", "dev_area_ha"]], on="rlb_id", how="left"
        )

        n_uplift, p_uplift, resi_n, gs_n, resi_p, gs_p = calculate_land_use_uplift(
            area_hectares=land_use_intersections["area_in_nn_catchment_ha"],
            dev_area_ha=land_use_intersections["dev_area_ha"],
            current_nitrogen_coeff=land_use_intersections["lu_curr_n_coeff"],
            residential_nitrogen_coeff=land_use_intersections["n_resi_coeff"],
            current_phosphorus_coeff=land_use_intersections["lu_curr_p_coeff"],
            residential_phosphorus_coeff=land_use_intersections["p_resi_coeff"],
            greenspace_config=self.config.greenspace,
        )
        land_use_intersections["n_lu_uplift"] = n_uplift
        land_use_intersections["p_lu_uplift"] = p_uplift

        n_post_suds, p_post_suds = apply_suds_mitigation(
            area_hectares=land_use_intersections["area_in_nn_catchment_ha"],
            dev_area_ha=land_use_intersections["dev_area_ha"],
            current_nitrogen_coeff=land_use_intersections["lu_curr_n_coeff"],
            current_phosphorus_coeff=land_use_intersections["lu_curr_p_coeff"],
            resi_n_component=resi_n,
            gs_n_component=gs_n,
            resi_p_component=resi_p,
            gs_p_component=gs_p,
            suds_config=self.config.suds,
        )
        land_use_intersections["n_lu_post_suds"] = n_post_suds
        land_use_intersections["p_lu_post_suds"] = p_post_suds

        uplift_sum = (
            land_use_intersections.groupby("rlb_id")
            .agg(
                {
                    "area_in_nn_catchment_ha": "sum",
                    "n_lu_uplift": "sum",
                    "p_lu_uplift": "sum",
                    "n_lu_post_suds": "sum",
                    "p_lu_post_suds": "sum",
                    "n2k_site_n": lambda x: "; ".join(sorted(set(x.dropna()))),
                }
            )
            .reset_index()
            .rename(columns={"n2k_site_n": "nn_catchment"})
        )

        rlb_gdf = rlb_gdf.merge(uplift_sum, on="rlb_id", how="left")

        return rlb_gdf

    def _calculate_wastewater_impacts(
        self, rlb_gdf: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        """Calculate wastewater treatment nutrient impacts."""
        logger.info("Calculating wastewater impacts")
        t_ww = time.perf_counter()

        dupes = rlb_gdf.columns[rlb_gdf.columns.duplicated()].tolist()
        if dupes:
            logger.warning(f"Dropping duplicate columns from rlb_gdf: {dupes}")
            rlb_gdf = rlb_gdf.loc[:, ~rlb_gdf.columns.duplicated()]

        t0 = time.perf_counter()
        stmt = (
            select(LookupTable)
            .where(LookupTable.name == "rates_lookup")
            .order_by(LookupTable.version.desc())
            .limit(1)
        )
        rates_lookup_obj = self.repository.execute_query(stmt, as_gdf=False)[0]
        rates_lookup = pd.DataFrame(rates_lookup_obj.data)

        rates_lookup = rates_lookup[
            ["nn_catchment", "occupancy_rate", "water_usage_L_per_person_day"]
        ].drop_duplicates(subset=["nn_catchment"])
        logger.info(
            f"[timing] wastewater: load rates_lookup: {time.perf_counter() - t0:.3f}s"
        )

        t0 = time.perf_counter()
        rlb_gdf = rlb_gdf.merge(rates_lookup, how="left", on="nn_catchment")

        if "wwtw_catchment" in rlb_gdf.columns:
            rates_by_catchment = rates_lookup.set_index("nn_catchment")[
                ["occupancy_rate", "water_usage_L_per_person_day"]
            ]
            mask = rlb_gdf["nn_catchment"].isna() & rlb_gdf["wwtw_catchment"].notna()
            for col in ["occupancy_rate", "water_usage_L_per_person_day"]:
                rlb_gdf.loc[mask, col] = rlb_gdf.loc[mask, "wwtw_catchment"].map(
                    rates_by_catchment[col]
                )

        rlb_gdf["daily_water_usage_L"] = rlb_gdf["dwellings"] * (
            rlb_gdf["occupancy_rate"] * rlb_gdf["water_usage_L_per_person_day"]
        )
        elapsed = time.perf_counter() - t0
        logger.info(
            f"[timing] wastewater: merge rates + fill + daily_water: {elapsed:.3f}s"
        )

        t0 = time.perf_counter()
        stmt = (
            select(LookupTable)
            .where(LookupTable.name == "wwtw_lookup")
            .order_by(LookupTable.version.desc())
            .limit(1)
        )
        wwtw_lookup_obj = self.repository.execute_query(stmt, as_gdf=False)[0]
        wwtw_lookup = pd.DataFrame(wwtw_lookup_obj.data)

        wwtw_lookup_cols = [
            "wwtw_code",
            "wwtw_name",
            "wwtw_catchment",
            "wwtw_subcatchment",
            "nitrogen_conc_2025_2030_mg_L",
            "nitrogen_conc_2030_onwards_mg_L",
            "phosphorus_conc_2025_2030_mg_L",
            "phosphorus_conc_2030_onwards_mg_L",
        ]
        available_cols = [c for c in wwtw_lookup_cols if c in wwtw_lookup.columns]
        wwtw_lookup = wwtw_lookup[available_cols]

        wwtw_lookup["wwtw_code"] = pd.to_numeric(
            wwtw_lookup["wwtw_code"], errors="coerce"
        ).astype("Int64")

        wwtw_lookup = wwtw_lookup.drop_duplicates(subset=["wwtw_code"])
        logger.info(
            f"[timing] wastewater: load wwtw_lookup: {time.perf_counter() - t0:.3f}s"
        )

        t0 = time.perf_counter()
        rlb_gdf = rlb_gdf.merge(
            wwtw_lookup, how="left", left_on="majority_wwtw_id", right_on="wwtw_code"
        )

        mask = (rlb_gdf["majority_wwtw_id"] == self.config.fallback_wwtw_id) & (
            rlb_gdf["wwtw_subcatchment"].isna()
        )
        rlb_gdf.loc[mask, "wwtw_subcatchment"] = rlb_gdf.loc[
            mask, "majority_opcat_name"
        ]

        rlb_gdf = rlb_gdf.drop(columns=["wwtw_code"], errors="ignore")

        cols_to_float = [
            "nitrogen_conc_2025_2030_mg_L",
            "nitrogen_conc_2030_onwards_mg_L",
            "phosphorus_conc_2025_2030_mg_L",
            "phosphorus_conc_2030_onwards_mg_L",
        ]
        rlb_gdf[cols_to_float] = rlb_gdf[cols_to_float].astype(float)
        elapsed = time.perf_counter() - t0
        logger.info(
            f"[timing] wastewater: merge wwtw + type conversion: {elapsed:.3f}s"
        )

        t0 = time.perf_counter()
        _, n_wwtw_temp, p_wwtw_temp = calculate_wastewater_load(
            dwellings=rlb_gdf["dwellings"],
            occupancy_rate=rlb_gdf["occupancy_rate"].fillna(0),
            water_usage_litres_per_person_per_day=rlb_gdf[
                "water_usage_L_per_person_day"
            ].fillna(0),
            nitrogen_conc_mg_per_litre=rlb_gdf["nitrogen_conc_2025_2030_mg_L"].fillna(
                0
            ),
            phosphorus_conc_mg_per_litre=rlb_gdf[
                "phosphorus_conc_2025_2030_mg_L"
            ].fillna(0),
        )
        rlb_gdf["n_wwtw_temp"] = n_wwtw_temp
        rlb_gdf["p_wwtw_temp"] = p_wwtw_temp

        _, n_wwtw_perm, p_wwtw_perm = calculate_wastewater_load(
            dwellings=rlb_gdf["dwellings"],
            occupancy_rate=rlb_gdf["occupancy_rate"].fillna(0),
            water_usage_litres_per_person_per_day=rlb_gdf[
                "water_usage_L_per_person_day"
            ].fillna(0),
            nitrogen_conc_mg_per_litre=rlb_gdf[
                "nitrogen_conc_2030_onwards_mg_L"
            ].fillna(0),
            phosphorus_conc_mg_per_litre=rlb_gdf[
                "phosphorus_conc_2030_onwards_mg_L"
            ].fillna(0),
        )
        rlb_gdf["n_wwtw_perm"] = n_wwtw_perm
        rlb_gdf["p_wwtw_perm"] = p_wwtw_perm
        elapsed = time.perf_counter() - t0
        logger.info(
            f"[timing] wastewater: calculate loads (vectorized): {elapsed:.3f}s"
        )
        logger.info(f"[timing] wastewater: TOTAL: {time.perf_counter() - t_ww:.3f}s")

        return rlb_gdf

    def _calculate_totals(self, rlb_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Calculate total impacts with precautionary buffer."""
        logger.info("Calculating totals with precautionary buffer")

        n_total, p_total = apply_buffer(
            nitrogen_land_use_post_suds=rlb_gdf["n_lu_post_suds"].fillna(0),
            phosphorus_land_use_post_suds=rlb_gdf["p_lu_post_suds"].fillna(0),
            nitrogen_wastewater=rlb_gdf["n_wwtw_perm"].fillna(0),
            phosphorus_wastewater=rlb_gdf["p_wwtw_perm"].fillna(0),
            precautionary_buffer_percent=self.config.precautionary_buffer_percent,
        )
        rlb_gdf["n_total"] = n_total
        rlb_gdf["p_total"] = p_total

        rlb_gdf["dwelling_density"] = rlb_gdf["dwellings"] / rlb_gdf["dev_area_ha"]

        round_cols = [
            "area_in_nn_catchment_ha",
            "n_wwtw_temp",
            "p_wwtw_temp",
            "n_wwtw_perm",
            "p_wwtw_perm",
            "n_total",
            "p_total",
        ]
        existing_round_cols = [col for col in round_cols if col in rlb_gdf.columns]
        rlb_gdf[existing_round_cols] = rlb_gdf[existing_round_cols].round(2)

        return rlb_gdf

    def _filter_out_of_scope(self, rlb_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Filter out developments that are out of scope."""
        rlb_gdf = rlb_gdf[
            ~(
                (rlb_gdf["area_in_nn_catchment_ha"].isna())
                & (rlb_gdf["wwtw_name"].isna())
            )
        ]

        return rlb_gdf[
            ~(
                (rlb_gdf["area_in_nn_catchment_ha"].isna())
                & (rlb_gdf["wwtw_name"] == "Package Treatment Plant default")
            )
        ]
