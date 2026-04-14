"""Unit tests for payload mapper."""

from app.clients.payload_mapper import build_quote_patch_payload
from app.models.domain import (
    Development,
    ImpactAssessmentResult,
    LandUseImpact,
    NutrientImpact,
    SpatialAssignment,
    WastewaterImpact,
)


def _make_result(
    n_total: float = 10.505,
    p_total: float = 2.304,
    nn_catchment: str | None = "Test Catchment",
    wwtw_id: int = 1,
):
    return ImpactAssessmentResult(
        rlb_id=1,
        development=Development(
            id="test-001",
            name="Test Dev",
            dwelling_category="housing",
            source="web_submission",
            dwellings=10,
            area_m2=5000.0,
            area_ha=0.5,
        ),
        spatial=SpatialAssignment(
            wwtw_id=wwtw_id,
            wwtw_name="Test WwTW",
            lpa_name="Test LPA",
            nn_catchment=nn_catchment,
            area_in_nn_catchment_ha=0.5,
        ),
        land_use=LandUseImpact(
            nitrogen_kg_yr=5.0,
            phosphorus_kg_yr=1.0,
        ),
        wastewater=WastewaterImpact(
            occupancy_rate=2.4,
            water_usage_L_per_person_day=110.0,
            daily_water_usage_L=2640.0,
            nitrogen_conc_2025_2030_mg_L=15.0,
            phosphorus_conc_2025_2030_mg_L=2.0,
            nitrogen_conc_2030_onwards_mg_L=10.0,
            phosphorus_conc_2030_onwards_mg_L=1.0,
            nitrogen_temp_kg_yr=4.0,
            phosphorus_temp_kg_yr=0.5,
            nitrogen_perm_kg_yr=3.0,
            phosphorus_perm_kg_yr=0.4,
        ),
        total=NutrientImpact(
            nitrogen_total_kg_yr=n_total,
            phosphorus_total_kg_yr=p_total,
        ),
    )


def test_build_payload_derives_edp_from_result():
    """Test building payload derives EDP from nn_catchment in assessment result."""
    result = _make_result(n_total=10.505, p_total=2.304)

    payload = build_quote_patch_payload([result])

    assert len(payload["edps"]) == 1
    edp_out = payload["edps"][0]
    assert edp_out["edpId"] == 1
    assert edp_out["edpName"] == "Test Catchment"
    assert edp_out["edpType"] == "NUTRIENT"
    assert edp_out["impact"]["nitrogenTotal"]["amount"] == 10.51  # NOSONAR
    assert edp_out["impact"]["nitrogenTotal"]["unit"] == "mg/I TP"
    assert edp_out["impact"]["nitrogenTotal"]["band"] == {"min": 4, "max": 4}
    assert edp_out["impact"]["phosphorusTotal"]["amount"] == 2.30  # NOSONAR
    assert edp_out["impact"]["phosphorusTotal"]["unit"] == "mg/I TP"
    assert edp_out["impact"]["phosphorusTotal"]["band"] == {"min": 3, "max": 3}
    assert edp_out["levyGbp"]["min"] == 999
    assert edp_out["levyGbp"]["max"] == 999


def test_build_payload_empty_results():
    """Test building payload with no results returns empty edps."""
    payload = build_quote_patch_payload([])
    assert payload == {"edps": []}


def test_build_payload_no_nn_catchment():
    """Test building payload when result has no nn_catchment returns empty edps."""
    result = _make_result(nn_catchment=None)
    payload = build_quote_patch_payload([result])
    assert payload == {"edps": []}


def test_build_payload_rounds_to_two_decimals():
    """Test that amounts are rounded to 2 decimal places."""
    result = _make_result(n_total=10.999, p_total=0.001)

    payload = build_quote_patch_payload([result])

    assert payload["edps"][0]["impact"]["nitrogenTotal"]["amount"] == 11.0  # NOSONAR
    assert payload["edps"][0]["impact"]["phosphorusTotal"]["amount"] == 0.0  # NOSONAR
