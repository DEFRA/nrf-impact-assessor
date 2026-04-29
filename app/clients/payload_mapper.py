"""Map assessment results to nrf-backend PATCH /quotes/{reference} payload."""

from app.clients.bands import get_band
from app.models.domain import CatchmentImpact, ImpactAssessmentResult
from app.models.enums import EdpType


def _amount_block(value: float) -> dict:
    band = get_band(value)
    return {
        "amount": value,
        "unit": "mg/I TP",
        "band": {"min": band, "max": band},
    }


def _impact_block(n_total: float, p_total: float) -> dict:
    return {
        "nitrogenTotal": _amount_block(round(n_total, 2)),
        "phosphorusTotal": _amount_block(round(p_total, 2)),
    }


def _edp_entry(catchment: CatchmentImpact) -> dict:
    return {
        "edpId": catchment.catchment_id,
        "edpName": catchment.catchment_name,
        "edpType": EdpType.NUTRIENT,
        "impact": _impact_block(
            catchment.nitrogen_total_kg_yr,
            catchment.phosphorus_total_kg_yr,
        ),
        # TODO: replace with real levy once finance calculation in place
        "levyGbp": {"min": 999, "max": 999},
    }


def build_quote_patch_payload(
    results: list[ImpactAssessmentResult],
) -> dict:
    """Build the PATCH body for nrf-backend from assessment results.

    Args:
        results: Assessment results (typically one per development).

    Returns:
        Dict matching the nrf-backend PATCH /quotes/{reference} schema.
        Returns {"edps": []} when there are no results or no catchment impacts.
    """
    if not results:
        return {"edps": []}

    result = results[0]
    if not result.catchment_impacts:
        return {"edps": []}

    edps = [_edp_entry(catchment) for catchment in result.catchment_impacts]

    return {"edps": edps}
