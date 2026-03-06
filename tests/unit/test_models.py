import pytest


def test_development_model_validation():
    """Test Development model validates inputs correctly."""
    from app.models import Development

    # Valid development
    dev = Development(
        id="DEV001",
        name="Test Site",
        dwelling_category="Large",
        source="LPA",
        dwellings=100,
        area_m2=50000,
        area_ha=5.0,
    )
    assert dev.dwellings == 100
    assert dev.area_ha == 5.0

    # Invalid: negative dwellings
    with pytest.raises(ValueError, match="greater than or equal to 0"):
        Development(
            id="DEV002",
            name="Invalid",
            dwelling_category="Small",
            source="LPA",
            dwellings=-10,  # Invalid
            area_m2=1000,
            area_ha=0.1,
        )


def test_assessment_result_model_helpers():
    """Test ImpactAssessmentResult helper methods."""
    from app.models import (
        Development,
        ImpactAssessmentResult,
        LandUseImpact,
        NutrientImpact,
        SpatialAssignment,
    )

    # Create minimal test result
    result = ImpactAssessmentResult(
        rlb_id=1,
        development=Development(
            id="DEV001",
            name="Test",
            dwelling_category="Medium",
            source="Test",
            dwellings=50,
            area_m2=10000,
            area_ha=1.0,
        ),
        spatial=SpatialAssignment(
            wwtw_id=123,
            wwtw_name="Test WwTW",
            wwtw_subcatchment="Test Sub",
            lpa_name="Test LPA",
            nn_catchment="Test NN Catchment",
            dev_subcatchment="Test Dev Sub",
            area_in_nn_catchment_ha=0.8,
        ),
        land_use=LandUseImpact(
            nitrogen_kg_yr=10.0,
            phosphorus_kg_yr=2.0,
            nitrogen_post_suds_kg_yr=7.5,
            phosphorus_post_suds_kg_yr=1.5,
        ),
        wastewater=None,
        total=NutrientImpact(nitrogen_total_kg_yr=9.0, phosphorus_total_kg_yr=1.8),
    )

    # Test helper methods
    assert result.is_within_nn_catchment() is True
    assert result.is_within_wwtw_catchment() is False  # wastewater is None
    assert result.requires_assessment() is True
