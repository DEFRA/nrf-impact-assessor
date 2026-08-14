"""Unit tests for payload mapper."""

from app.clients.payload_mapper import build_quote_patch_payload
from app.models.domain import (
    CatchmentImpact,
    Development,
    ImpactAssessmentResult,
    LandUseImpact,
    NutrientImpact,
    SpatialAssignment,
    WastewaterImpact,
)
from app.models.enums import EdpType


def _make_catchment_impact(
    name: str = "Test Catchment",
    n: float = 10.505,
    p: float = 2.304,
    catchment_id: str = "1",
) -> CatchmentImpact:
    return CatchmentImpact(
        catchment_id=catchment_id,
        catchment_name=name,
        nitrogen_total_kg_yr=n,
        phosphorus_total_kg_yr=p,
    )


def _make_result(
    n_total: float = 10.505,
    p_total: float = 2.304,
    nn_catchment: str | None = "Test Catchment",
    wwtw_id: int = 1,
    catchment_impacts: list[CatchmentImpact] | None = None,
):
    if catchment_impacts is None:
        catchment_impacts = (
            [_make_catchment_impact(nn_catchment, n_total, p_total)]
            if nn_catchment
            else []
        )
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
        catchment_impacts=catchment_impacts,
    )


EDP_LABEL = "Broads SAC (Yare & Bure) & Wensum SAC"


def test_build_payload_derives_edp_from_result():
    """Single catchment produces one EDP named for the EDP, not the catchment."""
    result = _make_result(n_total=10.505, p_total=2.304)

    payload = build_quote_patch_payload([result], [EDP_LABEL])

    assert len(payload["edps"]) == 1
    edp_out = payload["edps"][0]
    assert edp_out["edpId"] == "1"
    assert edp_out["edpName"] == EDP_LABEL
    assert edp_out["edpType"] == EdpType.NUTRIENT.value
    assert edp_out["impact"]["nitrogenTotal"]["amount"] == 10.51  # NOSONAR
    assert edp_out["impact"]["nitrogenTotal"]["unit"] == "mg/I TP"
    assert edp_out["impact"]["nitrogenTotal"]["band"] == {"min": 4, "max": 4}
    assert edp_out["impact"]["phosphorusTotal"]["amount"] == 2.30  # NOSONAR
    assert edp_out["impact"]["phosphorusTotal"]["unit"] == "mg/I TP"
    assert edp_out["impact"]["phosphorusTotal"]["band"] == {"min": 3, "max": 3}
    assert edp_out["levyGbp"]["min"] == 999
    assert edp_out["levyGbp"]["max"] == 999


def test_build_payload_excludes_top_level_totals():
    """Payload must not include totalNitrogen/totalPhosphorus (rejected by backend)."""
    result = _make_result(n_total=10.505, p_total=2.304)

    payload = build_quote_patch_payload([result], [EDP_LABEL])

    assert "totalNitrogen" not in payload
    assert "totalPhosphorus" not in payload
    assert set(payload.keys()) == {"edps"}


def _two_catchment_result():
    """A development spanning two NN catchments of the same EDP."""
    return _make_result(
        n_total=20.0,
        p_total=2.0,
        nn_catchment=None,
        catchment_impacts=[
            CatchmentImpact(
                catchment_id="11",
                catchment_name="Wensum",
                nitrogen_total_kg_yr=20.0,
                phosphorus_total_kg_yr=2.0,
            ),
            CatchmentImpact(
                catchment_id="10",
                catchment_name="Broads",
                nitrogen_total_kg_yr=20.0,
                phosphorus_total_kg_yr=2.0,
            ),
        ],
    )


def test_build_payload_multiple_catchments_one_edp():
    """Two catchments of one EDP collapse to a single entry, totals counted once.

    The per-catchment figures are development-level totals, so emitting one
    entry per catchment duplicated them and made nrf-backend's levy sum
    double-count.
    """
    payload = build_quote_patch_payload([_two_catchment_result()], [EDP_LABEL])

    assert len(payload["edps"]) == 1
    edp_out = payload["edps"][0]
    assert edp_out["edpName"] == EDP_LABEL
    assert edp_out["edpType"] == EdpType.NUTRIENT.value
    assert edp_out["impact"]["nitrogenTotal"]["amount"] == 20.0  # NOSONAR
    assert edp_out["impact"]["phosphorusTotal"]["amount"] == 2.0  # NOSONAR


def test_build_payload_edp_id_is_lowest_catchment_id():
    """edpId stays provisional (lowest catchment OID) but must be deterministic."""
    payload = build_quote_patch_payload([_two_catchment_result()], [EDP_LABEL])

    assert payload["edps"][0]["edpId"] == "10"


def test_build_payload_no_edp_labels():
    """Without an EDP label there is nothing to name the entry, so emit none."""
    result = _make_result()

    payload = build_quote_patch_payload([result], [])

    assert payload == {"edps": []}


def test_build_payload_multiple_edp_labels():
    """Two EDPs cannot be attributed: totals are per development, not per EDP."""
    result = _make_result()

    payload = build_quote_patch_payload([result], [EDP_LABEL, "Norfolk EDP 2"])

    assert payload == {"edps": []}


def test_build_payload_empty_results():
    """Empty results list returns empty edps with no top-level totals."""
    payload = build_quote_patch_payload([], [EDP_LABEL])
    assert payload == {"edps": []}


def test_build_payload_no_nn_catchment():
    """Result with no catchment_impacts returns empty edps."""
    result = _make_result(catchment_impacts=[])
    payload = build_quote_patch_payload([result], [EDP_LABEL])
    assert payload == {"edps": []}


def test_build_payload_rounds_to_two_decimals():
    """Amounts in EDPs and top-level totals are rounded to 2 decimal places."""
    result = _make_result(n_total=10.999, p_total=0.001)

    payload = build_quote_patch_payload([result], [EDP_LABEL])

    assert payload["edps"][0]["impact"]["nitrogenTotal"]["amount"] == 11.0  # NOSONAR
    assert payload["edps"][0]["impact"]["phosphorusTotal"]["amount"] == 0.0  # NOSONAR
