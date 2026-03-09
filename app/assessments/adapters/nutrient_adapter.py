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


def _opt_float(row: pd.Series, key: str) -> float | None:
    """Return float(row[key]) or None if the value is NA."""
    val = row.get(key)
    return float(val) if pd.notna(val) else None


def _opt_str(row: pd.Series, key: str) -> str | None:
    """Return row[key] or None if the value is NA."""
    val = row.get(key)
    return val if pd.notna(val) else None


def _build_wastewater(row: pd.Series) -> WastewaterImpact | None:
    """Build WastewaterImpact from a row, or None if outside WwTW catchment."""
    if not pd.notna(row.get("wwtw_name")):
        return None
    return WastewaterImpact(
        occupancy_rate=_opt_float(row, "occupancy_rate"),
        water_usage_L_per_person_day=_opt_float(row, "water_usage_L_per_person_day"),
        daily_water_usage_L=_opt_float(row, "daily_water_usage_L"),
        nitrogen_conc_2025_2030_mg_L=_opt_float(row, "nitrogen_conc_2025_2030_mg_L"),
        phosphorus_conc_2025_2030_mg_L=_opt_float(row, "phosphorus_conc_2025_2030_mg_L"),
        nitrogen_conc_2030_onwards_mg_L=_opt_float(row, "nitrogen_conc_2030_onwards_mg_L"),
        phosphorus_conc_2030_onwards_mg_L=_opt_float(row, "phosphorus_conc_2030_onwards_mg_L"),
        nitrogen_temp_kg_yr=_opt_float(row, "n_wwtw_temp"),
        phosphorus_temp_kg_yr=_opt_float(row, "p_wwtw_temp"),
        nitrogen_perm_kg_yr=_opt_float(row, "n_wwtw_perm"),
        phosphorus_perm_kg_yr=_opt_float(row, "p_wwtw_perm"),
    )


def _row_to_result(row: pd.Series) -> ImpactAssessmentResult:
    """Convert a single DataFrame row to ImpactAssessmentResult."""
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
        wwtw_name=_opt_str(row, "wwtw_name"),
        wwtw_subcatchment=_opt_str(row, "wwtw_subcatchment"),
        lpa_name=row["majority_name"],
        nn_catchment=_opt_str(row, "nn_catchment"),
        dev_subcatchment=_opt_str(row, "majority_opcat_name"),
        area_in_nn_catchment_ha=_opt_float(row, "area_in_nn_catchment_ha"),
    )

    land_use = LandUseImpact(
        nitrogen_kg_yr=_opt_float(row, "n_lu_uplift"),
        phosphorus_kg_yr=_opt_float(row, "p_lu_uplift"),
        nitrogen_post_suds_kg_yr=_opt_float(row, "n_lu_post_suds"),
        phosphorus_post_suds_kg_yr=_opt_float(row, "p_lu_post_suds"),
    )

    total = NutrientImpact(
        nitrogen_total_kg_yr=float(row["n_total"]),
        phosphorus_total_kg_yr=float(row["p_total"]),
    )

    return ImpactAssessmentResult(
        rlb_id=int(row["rlb_id"]),
        development=development,
        spatial=spatial,
        land_use=land_use,
        wastewater=_build_wastewater(row),
        total=total,
    )
