"""Unit tests for nutrient adapter module."""

import pandas as pd
import pytest

from app.assessments.adapters.nutrient_adapter import to_domain_models
from app.models.domain import (
    Development,
    ImpactAssessmentResult,
    LandUseImpact,
    NutrientImpact,
    SpatialAssignment,
    WastewaterImpact,
)


@pytest.fixture
def sample_impact_summary():
    """Create a sample impact summary DataFrame."""
    return pd.DataFrame(
        [
            {
                "rlb_id": 1,
                "id": "site_001",
                "name": "Test Development 1",
                "dwelling_category": "Small",
                "source": "LPA",
                "dwellings": 10,
                "shape_area": 5000.0,
                "dev_area_ha": 0.5,
                "majority_wwtw_id": 123,
                "wwtw_name": "Test WwTW",
                "wwtw_subcatchment": "Test Subcatchment",
                "majority_name": "Test LPA",
                "nn_catchment": "Solent",
                "majority_opcat_name": "Operational Catchment",
                "area_in_nn_catchment_ha": 0.3,
                "n_lu_uplift": 5.25,
                "p_lu_uplift": 0.45,
                "n_lu_post_suds": 4.72,
                "p_lu_post_suds": 0.41,
                "occupancy_rate": 2.4,
                "water_usage_L_per_person_day": 150.0,
                "daily_water_usage_L": 3600.0,
                "nitrogen_conc_2025_2030_mg_L": 10.0,
                "phosphorus_conc_2025_2030_mg_L": 1.0,
                "nitrogen_conc_2030_onwards_mg_L": 8.0,
                "phosphorus_conc_2030_onwards_mg_L": 0.8,
                "n_wwtw_temp": 13.14,
                "p_wwtw_temp": 1.31,
                "n_wwtw_perm": 10.51,
                "p_wwtw_perm": 1.05,
                "n_total": 18.35,
                "p_total": 1.75,
            },
            {
                "rlb_id": 2,
                "id": "site_002",
                "name": "Test Development 2",
                "dwelling_category": "Medium",
                "source": "LPA",
                "dwellings": 20,
                "shape_area": 10000.0,
                "dev_area_ha": 1.0,
                "majority_wwtw_id": 456,
                "wwtw_name": None,  # Outside WwTW catchment
                "wwtw_subcatchment": None,
                "majority_name": "Test LPA",
                "nn_catchment": "Solent",
                "majority_opcat_name": "Operational Catchment",
                "area_in_nn_catchment_ha": 0.8,
                "n_lu_uplift": 12.50,
                "p_lu_uplift": 1.15,
                "n_lu_post_suds": 11.25,
                "p_lu_post_suds": 1.04,
                "occupancy_rate": None,
                "water_usage_L_per_person_day": None,
                "daily_water_usage_L": None,
                "nitrogen_conc_2025_2030_mg_L": None,
                "phosphorus_conc_2025_2030_mg_L": None,
                "nitrogen_conc_2030_onwards_mg_L": None,
                "phosphorus_conc_2030_onwards_mg_L": None,
                "n_wwtw_temp": None,
                "p_wwtw_temp": None,
                "n_wwtw_perm": None,
                "p_wwtw_perm": None,
                "n_total": 13.50,
                "p_total": 1.25,
            },
        ]
    )


def test_to_domain_models_basic(sample_impact_summary):
    """Test basic conversion to domain models."""
    dataframes = {"impact_summary": sample_impact_summary}

    result = to_domain_models(dataframes)

    assert isinstance(result, dict)
    assert "assessment_results" in result
    assert isinstance(result["assessment_results"], list)
    assert len(result["assessment_results"]) == 2

    # All results should be ImpactAssessmentResult
    for item in result["assessment_results"]:
        assert isinstance(item, ImpactAssessmentResult)


def test_to_domain_models_converts_all_components(sample_impact_summary):
    """Test that all domain model components are converted correctly."""
    dataframes = {"impact_summary": sample_impact_summary}

    result = to_domain_models(dataframes)
    first = result["assessment_results"][0]

    # Verify all components exist and have correct types
    assert isinstance(first.development, Development)
    assert isinstance(first.spatial, SpatialAssignment)
    assert isinstance(first.land_use, LandUseImpact)
    assert isinstance(first.wastewater, WastewaterImpact)
    assert isinstance(first.total, NutrientImpact)


def test_development_conversion(sample_impact_summary):
    """Test Development model conversion."""
    dataframes = {"impact_summary": sample_impact_summary}

    result = to_domain_models(dataframes)
    dev = result["assessment_results"][0].development

    assert dev.id == "site_001"
    assert dev.name == "Test Development 1"
    assert dev.dwelling_category == "Small"
    assert dev.source == "LPA"
    assert dev.dwellings == 10
    assert dev.area_m2 == 5000.0
    assert dev.area_ha == 0.5


def test_spatial_assignment_conversion(sample_impact_summary):
    """Test SpatialAssignment model conversion."""
    dataframes = {"impact_summary": sample_impact_summary}

    result = to_domain_models(dataframes)
    spatial = result["assessment_results"][0].spatial

    assert spatial.wwtw_id == 123
    assert spatial.wwtw_name == "Test WwTW"
    assert spatial.wwtw_subcatchment == "Test Subcatchment"
    assert spatial.lpa_name == "Test LPA"
    assert spatial.nn_catchment == "Solent"
    assert spatial.dev_subcatchment == "Operational Catchment"
    assert spatial.area_in_nn_catchment_ha == 0.3


def test_land_use_impact_conversion(sample_impact_summary):
    """Test LandUseImpact model conversion."""
    dataframes = {"impact_summary": sample_impact_summary}

    result = to_domain_models(dataframes)
    land_use = result["assessment_results"][0].land_use

    assert land_use.nitrogen_kg_yr == 5.25
    assert land_use.phosphorus_kg_yr == 0.45
    assert land_use.nitrogen_post_suds_kg_yr == 4.72
    assert land_use.phosphorus_post_suds_kg_yr == 0.41


def test_wastewater_impact_conversion(sample_impact_summary):
    """Test WastewaterImpact model conversion."""
    dataframes = {"impact_summary": sample_impact_summary}

    result = to_domain_models(dataframes)
    wastewater = result["assessment_results"][0].wastewater

    assert wastewater.occupancy_rate == 2.4
    assert wastewater.water_usage_L_per_person_day == 150.0
    assert wastewater.daily_water_usage_L == 3600.0
    assert wastewater.nitrogen_conc_2025_2030_mg_L == 10.0
    assert wastewater.phosphorus_conc_2025_2030_mg_L == 1.0
    assert wastewater.nitrogen_conc_2030_onwards_mg_L == 8.0
    assert wastewater.phosphorus_conc_2030_onwards_mg_L == 0.8
    assert wastewater.nitrogen_temp_kg_yr == 13.14
    assert wastewater.phosphorus_temp_kg_yr == 1.31
    assert wastewater.nitrogen_perm_kg_yr == 10.51
    assert wastewater.phosphorus_perm_kg_yr == 1.05


def test_nutrient_impact_conversion(sample_impact_summary):
    """Test NutrientImpact model conversion."""
    dataframes = {"impact_summary": sample_impact_summary}

    result = to_domain_models(dataframes)
    total = result["assessment_results"][0].total

    assert total.nitrogen_total_kg_yr == 18.35
    assert total.phosphorus_total_kg_yr == 1.75


def test_wastewater_none_when_outside_catchment(sample_impact_summary):
    """Test that wastewater is None when outside WwTW catchment."""
    dataframes = {"impact_summary": sample_impact_summary}

    result = to_domain_models(dataframes)
    # Second development has no WwTW
    wastewater = result["assessment_results"][1].wastewater

    assert wastewater is None


def test_handles_null_values(sample_impact_summary):
    """Test that adapter handles null/NaN values correctly."""
    dataframes = {"impact_summary": sample_impact_summary}

    result = to_domain_models(dataframes)
    spatial = result["assessment_results"][1].spatial

    # WwTW fields should be None
    assert spatial.wwtw_name is None
    assert spatial.wwtw_subcatchment is None

    # Other fields should still be populated
    assert spatial.nn_catchment == "Solent"
    assert spatial.area_in_nn_catchment_ha == 0.8


def test_handles_empty_dataframe():
    """Test that adapter handles empty DataFrame."""
    empty_df = pd.DataFrame(
        columns=[
            "rlb_id",
            "id",
            "name",
            "dwelling_category",
            "source",
            "dwellings",
            "shape_area",
            "dev_area_ha",
            "majority_wwtw_id",
            "wwtw_name",
            "majority_name",
            "nn_catchment",
            "majority_opcat_name",
            "area_in_nn_catchment_ha",
            "n_lu_uplift",
            "p_lu_uplift",
            "n_lu_post_suds",
            "p_lu_post_suds",
            "n_wwtw_perm",
            "p_wwtw_perm",
            "n_total",
            "p_total",
        ]
    )

    dataframes = {"impact_summary": empty_df}
    result = to_domain_models(dataframes)

    assert result["assessment_results"] == []


def test_handles_empty_name():
    """Test that adapter defaults empty name to empty string."""
    df = pd.DataFrame(
        [
            {
                "rlb_id": 1,
                "id": "site_001",
                "name": None,
                "dwelling_category": "Small",
                "source": "LPA",
                "dwellings": 10,
                "shape_area": 5000.0,
                "dev_area_ha": 0.5,
                "majority_wwtw_id": 123,
                "wwtw_name": None,
                "wwtw_subcatchment": None,
                "majority_name": "Test LPA",
                "nn_catchment": "Solent",
                "majority_opcat_name": None,
                "area_in_nn_catchment_ha": 0.3,
                "n_lu_uplift": 5.25,
                "p_lu_uplift": 0.45,
                "n_lu_post_suds": 4.72,
                "p_lu_post_suds": 0.41,
                "n_wwtw_perm": None,
                "p_wwtw_perm": None,
                "n_total": 10.0,
                "p_total": 1.0,
            }
        ]
    )

    dataframes = {"impact_summary": df}
    result = to_domain_models(dataframes)
    dev = result["assessment_results"][0].development

    assert dev.name == ""


def test_preserves_rlb_id(sample_impact_summary):
    """Test that rlb_id is preserved in result."""
    dataframes = {"impact_summary": sample_impact_summary}

    result = to_domain_models(dataframes)

    assert result["assessment_results"][0].rlb_id == 1
    assert result["assessment_results"][1].rlb_id == 2


def test_handles_partial_land_use():
    """Test that adapter handles developments outside NN catchment (no land use)."""
    df = pd.DataFrame(
        [
            {
                "rlb_id": 1,
                "id": "site_001",
                "name": "Test",
                "dwelling_category": "Small",
                "source": "LPA",
                "dwellings": 10,
                "shape_area": 5000.0,
                "dev_area_ha": 0.5,
                "majority_wwtw_id": 123,
                "wwtw_name": "Test WwTW",
                "wwtw_subcatchment": None,
                "majority_name": "Test LPA",
                "nn_catchment": None,
                "majority_opcat_name": None,
                "area_in_nn_catchment_ha": None,
                "n_lu_uplift": None,
                "p_lu_uplift": None,
                "n_lu_post_suds": None,
                "p_lu_post_suds": None,
                "n_wwtw_perm": 10.0,
                "p_wwtw_perm": 1.0,
                "n_total": 12.0,
                "p_total": 1.2,
            }
        ]
    )

    dataframes = {"impact_summary": df}
    result = to_domain_models(dataframes)
    land_use = result["assessment_results"][0].land_use

    # Land use impacts should be None when outside NN catchment
    assert land_use.nitrogen_kg_yr is None
    assert land_use.phosphorus_kg_yr is None
    assert land_use.nitrogen_post_suds_kg_yr is None
    assert land_use.phosphorus_post_suds_kg_yr is None
