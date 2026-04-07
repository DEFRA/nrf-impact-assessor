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
from app.models.job import EdpInput, LevyRange


def _make_result(n_total: float = 10.505, p_total: float = 2.304):
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
            wwtw_id=1,
            wwtw_name="Test WwTW",
            lpa_name="Test LPA",
            nn_catchment="Test Catchment",
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


def _make_edp(edp_id=1, edp_name="Somerset Levels", min_levy=1000.0, max_levy=2000.0):
    return EdpInput(
        edp_id=edp_id,
        edp_name=edp_name,
        edp_type="NUTRIENT",
        levy_gbp=LevyRange(min=min_levy, max=max_levy),
    )


def test_build_payload_single_edp():
    """Test building payload with one EDP and one result."""
    result = _make_result(n_total=10.505, p_total=2.304)
    edp = _make_edp()

    payload = build_quote_patch_payload([result], [edp])

    assert len(payload["edps"]) == 1
    edp_out = payload["edps"][0]
    assert edp_out["edpId"] == 1
    assert edp_out["edpName"] == "Somerset Levels"
    assert edp_out["edpType"] == "NUTRIENT"
    assert edp_out["impact"]["nitrogenTotal"]["amount"] == 10.51  # NOSONAR
    assert edp_out["impact"]["nitrogenTotal"]["unit"] == "mg/I TP"
    assert edp_out["impact"]["phosphorusTotal"]["amount"] == 2.30  # NOSONAR
    assert edp_out["impact"]["phosphorusTotal"]["unit"] == "mg/I TP"
    assert edp_out["levyGbp"]["min"] == 1000.00  # NOSONAR
    assert edp_out["levyGbp"]["max"] == 2000.00  # NOSONAR


def test_build_payload_multiple_edps():
    """Test building payload with multiple EDPs — all get same totals."""
    result = _make_result(n_total=5.0, p_total=1.0)
    edps = [
        _make_edp(edp_id=1, edp_name="EDP A", min_levy=500, max_levy=1000),
        _make_edp(edp_id=2, edp_name="EDP B", min_levy=750, max_levy=1500),
    ]

    payload = build_quote_patch_payload([result], edps)

    assert len(payload["edps"]) == 2
    assert payload["edps"][0]["edpId"] == 1
    assert payload["edps"][1]["edpId"] == 2
    # Both EDPs get the same impact totals
    assert payload["edps"][0]["impact"]["nitrogenTotal"]["amount"] == 5.0  # NOSONAR
    assert payload["edps"][1]["impact"]["nitrogenTotal"]["amount"] == 5.0  # NOSONAR


def test_build_payload_empty_results():
    """Test building payload with no results returns empty edps."""
    edp = _make_edp()
    payload = build_quote_patch_payload([], [edp])
    assert payload == {"edps": []}


def test_build_payload_rounds_to_two_decimals():
    """Test that amounts are rounded to 2 decimal places."""
    result = _make_result(n_total=10.999, p_total=0.001)
    edp = _make_edp()

    payload = build_quote_patch_payload([result], [edp])

    assert payload["edps"][0]["impact"]["nitrogenTotal"]["amount"] == 11.0  # NOSONAR
    assert payload["edps"][0]["impact"]["phosphorusTotal"]["amount"] == 0.0  # NOSONAR
