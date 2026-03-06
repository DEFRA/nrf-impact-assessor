"""Convert nutrient assessment results to domain models.

This adapter transforms DataFrame results from the nutrient assessment into
typed Pydantic domain models for persistence and API output.
"""

import pandas as pd

from app.config import RequiredColumns
from app.models.domain import (
    Development,
    ImpactAssessmentResult,
    LandUseImpact,
    NutrientImpact,
    SpatialAssignment,
    WastewaterImpact,
)


def to_domain_models(dataframes: dict) -> dict:
    """Convert nutrient DataFrames to Pydantic models.

    Args:
        dataframes: Dict from nutrient.run() with keys:
            - "impact_summary": DataFrame with all impact calculations

    Returns:
        Dict with typed domain models:
        {
            "assessment_results": List[ImpactAssessmentResult]
        }
    """
    impact_df = dataframes["impact_summary"]

    results = [_row_to_result(row) for _, row in impact_df.iterrows()]

    return {"assessment_results": results}


def _row_to_result(row: pd.Series) -> ImpactAssessmentResult:
    """Convert a single DataFrame row to ImpactAssessmentResult.

    Args:
        row: Single row from processed DataFrame

    Returns:
        ImpactAssessmentResult domain model
    """
    development = Development(
        id=str(row["id"]),
        name=row["name"] if pd.notna(row["name"]) else "",
        dwelling_category=row["dwelling_category"],
        source=row["source"],
        dwellings=int(row["dwellings"]),
        area_m2=float(row[RequiredColumns.SHAPE_AREA]),
        area_ha=float(row["dev_area_ha"]),
    )

    spatial = SpatialAssignment(
        wwtw_id=int(row["majority_wwtw_id"]),
        wwtw_name=row["wwtw_name"] if pd.notna(row["wwtw_name"]) else None,
        wwtw_subcatchment=row["wwtw_subcatchment"]
        if pd.notna(row["wwtw_subcatchment"])
        else None,
        lpa_name=row["majority_name"],
        nn_catchment=row["nn_catchment"] if pd.notna(row["nn_catchment"]) else None,
        dev_subcatchment=row["majority_opcat_name"]
        if pd.notna(row["majority_opcat_name"])
        else None,
        area_in_nn_catchment_ha=float(row["area_in_nn_catchment_ha"])
        if pd.notna(row["area_in_nn_catchment_ha"])
        else None,
    )

    land_use = LandUseImpact(
        nitrogen_kg_yr=float(row["n_lu_uplift"])
        if pd.notna(row["n_lu_uplift"])
        else None,
        phosphorus_kg_yr=float(row["p_lu_uplift"])
        if pd.notna(row["p_lu_uplift"])
        else None,
        nitrogen_post_suds_kg_yr=float(row["n_lu_post_suds"])
        if pd.notna(row["n_lu_post_suds"])
        else None,
        phosphorus_post_suds_kg_yr=float(row["p_lu_post_suds"])
        if pd.notna(row["p_lu_post_suds"])
        else None,
    )

    # WastewaterImpact model (None if outside WwTW catchment)
    # Create wastewater impact whenever WwTW is assigned, even if rates are missing
    # This ensures we output WwTW permit concentrations for reporting
    wastewater = None
    if pd.notna(row.get("wwtw_name")):
        # Extract concentration values (from WwTW lookup)
        n_conc_2025_2030 = (
            float(row.get("nitrogen_conc_2025_2030_mg_L"))
            if pd.notna(row.get("nitrogen_conc_2025_2030_mg_L"))
            else None
        )
        p_conc_2025_2030 = (
            float(row.get("phosphorus_conc_2025_2030_mg_L"))
            if pd.notna(row.get("phosphorus_conc_2025_2030_mg_L"))
            else None
        )
        n_conc_2030_onwards = (
            float(row.get("nitrogen_conc_2030_onwards_mg_L"))
            if pd.notna(row.get("nitrogen_conc_2030_onwards_mg_L"))
            else None
        )
        p_conc_2030_onwards = (
            float(row.get("phosphorus_conc_2030_onwards_mg_L"))
            if pd.notna(row.get("phosphorus_conc_2030_onwards_mg_L"))
            else None
        )

        # Extract calculated loads (can be None if rates were missing)
        n_temp = (
            float(row.get("n_wwtw_temp")) if pd.notna(row.get("n_wwtw_temp")) else None
        )
        p_temp = (
            float(row.get("p_wwtw_temp")) if pd.notna(row.get("p_wwtw_temp")) else None
        )
        n_perm = (
            float(row.get("n_wwtw_perm")) if pd.notna(row.get("n_wwtw_perm")) else None
        )
        p_perm = (
            float(row.get("p_wwtw_perm")) if pd.notna(row.get("p_wwtw_perm")) else None
        )

        # Extract rates and usage (can be None if outside NN catchment)
        occ_rate = (
            float(row.get("occupancy_rate"))
            if pd.notna(row.get("occupancy_rate"))
            else None
        )
        water_usage = (
            float(row.get("water_usage_L_per_person_day"))
            if pd.notna(row.get("water_usage_L_per_person_day"))
            else None
        )
        daily_usage = (
            float(row.get("daily_water_usage_L"))
            if pd.notna(row.get("daily_water_usage_L"))
            else None
        )

        wastewater = WastewaterImpact(
            occupancy_rate=occ_rate,
            water_usage_L_per_person_day=water_usage,
            daily_water_usage_L=daily_usage,
            nitrogen_conc_2025_2030_mg_L=n_conc_2025_2030,
            phosphorus_conc_2025_2030_mg_L=p_conc_2025_2030,
            nitrogen_conc_2030_onwards_mg_L=n_conc_2030_onwards,
            phosphorus_conc_2030_onwards_mg_L=p_conc_2030_onwards,
            nitrogen_temp_kg_yr=n_temp,
            phosphorus_temp_kg_yr=p_temp,
            nitrogen_perm_kg_yr=n_perm,
            phosphorus_perm_kg_yr=p_perm,
        )

    # NutrientImpact model (totals always present, uses 0 for missing)
    total = NutrientImpact(
        nitrogen_total_kg_yr=float(row["n_total"]),
        phosphorus_total_kg_yr=float(row["p_total"]),
    )

    return ImpactAssessmentResult(
        rlb_id=int(row["rlb_id"]),
        development=development,
        spatial=spatial,
        land_use=land_use,
        wastewater=wastewater,
        total=total,
    )
