"""Convert GCN assessment results to domain models.

This adapter transforms DataFrame results from the GCN assessment into
typed Pydantic domain models for persistence and API output.
"""

import pandas as pd

from app.models.domain import (
    GcnAssessmentResult,
    GcnDevelopment,
    GcnHabitatImpact,
    GcnPondFrequency,
    GcnPondInfo,
)


def to_domain_models(dataframes: dict) -> dict:
    """Convert GCN DataFrames to Pydantic models.

    Args:
        dataframes: Dict from gcn.run() with keys:
            - "habitat_impact": DataFrame with habitat impact by risk zone
            - "pond_frequency": DataFrame with pond counts by zone/status
            - "rlb_data": Processed RLB GeoDataFrame
            - "all_ponds_data": Combined ponds GeoDataFrame
            - "pond_zones_data": DataFrame with detailed pond zone information
            - "unique_ref": Unique run reference

    Returns:
        Dict with typed domain models:
        {
            "assessment_results": List[GcnAssessmentResult]
        }
    """
    habitat_impact_df = dataframes["habitat_impact"]
    pond_frequency_df = dataframes["pond_frequency"]
    rlb_df = dataframes["rlb_data"]
    all_ponds_df = dataframes["all_ponds_data"]
    pond_zones_df = dataframes["pond_zones_data"]
    unique_ref = dataframes["unique_ref"]

    # Create GcnDevelopment model
    development: GcnDevelopment
    if not rlb_df.empty:
        first_rlb_row = rlb_df.iloc[0]
        development = GcnDevelopment(
            id=str(first_rlb_row["id"]),
            name=first_rlb_row["name"]
            if "name" in first_rlb_row and pd.notna(first_rlb_row["name"])
            else None,
            unique_ref=unique_ref,
            unique_site=first_rlb_row["UniqueSite"],
            unique_buffer_site=first_rlb_row["UniqueBufferSite"]
            if "UniqueBufferSite" in first_rlb_row
            and pd.notna(first_rlb_row["UniqueBufferSite"])
            else None,
            area=first_rlb_row["Area"],
            orig_fid=int(first_rlb_row["orig_fid"]),
        )
    else:
        # Fallback for empty RLB (should ideally be caught earlier)
        development = GcnDevelopment(
            id="unknown",
            name=None,
            unique_ref=unique_ref,
            unique_site=f"{unique_ref}_Site00000",
            unique_buffer_site=None,
            area="RLB",
            orig_fid=0,
        )

    # Convert habitat_impact_df to list of GcnHabitatImpact
    habitat_impacts = [
        GcnHabitatImpact(
            area=row["Area"],
            risk_zone=row["RZ"],
            shape_area=float(row["Shape_Area"]),
        )
        for _, row in habitat_impact_df.iterrows()
    ]

    # Convert pond_frequency_df to list of GcnPondFrequency
    pond_frequencies = [
        GcnPondFrequency(
            pans=row["PANS"],
            area=row["Area"],
            max_zone=row["MaxZone"],
            tmp_imp=row["TmpImp"],
            frequency=int(row["FREQUENCY"]),
        )
        for _, row in pond_frequency_df.iterrows()
    ]

    # Convert all_ponds_df and pond_zones_df to lists of GcnPondInfo
    ponds_detailed = pd.merge(
        all_ponds_df.drop(columns=["geometry"], errors="ignore"),
        pond_zones_df[["Pond_ID", "CONCATENATE_RZ", "MaxZone"]],
        on="Pond_ID",
        how="left",
    )

    ponds_in_rlb_list = [
        GcnPondInfo(
            pond_id=row["Pond_ID"],
            pans=row["PANS"],
            tmp_imp=row["TmpImp"],
            area=row["Area"],
            concatenate_rz=row["CONCATENATE_RZ"],
            max_zone=row["MaxZone"],
        )
        for _, row in ponds_detailed[ponds_detailed["Area"] == "RLB"].iterrows()
    ]

    ponds_in_buffer_list = [
        GcnPondInfo(
            pond_id=row["Pond_ID"],
            pans=row["PANS"],
            tmp_imp=row["TmpImp"],
            area=row["Area"],
            concatenate_rz=row["CONCATENATE_RZ"],
            max_zone=row["MaxZone"],
        )
        for _, row in ponds_detailed[ponds_detailed["Area"] == "Buffer"].iterrows()
    ]

    # Create GcnAssessmentResult
    gcn_result = GcnAssessmentResult(
        unique_ref=unique_ref,
        development=development,
        habitat_impacts=habitat_impacts,
        pond_frequencies=pond_frequencies,
        ponds_in_rlb=ponds_in_rlb_list,
        ponds_in_buffer=ponds_in_buffer_list,
    )

    return {"assessment_results": [gcn_result]}
