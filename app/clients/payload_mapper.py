"""Map assessment results to nrf-backend PATCH /quotes/{reference} payload."""

from app.clients.bands import get_band
from app.models.domain import CatchmentImpact, ImpactAssessmentResult


def _impact_block(n_total: float, p_total: float) -> dict:
    n = round(n_total, 2)
    p = round(p_total, 2)
    return {
        "nitrogenTotal": {
            "amount": n,
            "unit": "mg/I TP",
            "band": {"min": get_band(n), "max": get_band(n)},
        },
        "phosphorusTotal": {
            "amount": p,
            "unit": "mg/I TP",
            "band": {"min": get_band(p), "max": get_band(p)},
        },
    }


def _edp_entry(wwtw_id: int, catchment: CatchmentImpact) -> dict:
    return {
        "edpId": wwtw_id,
        "edpName": catchment.catchment_name,
        "edpType": "NUTRIENT",
        "impact": _impact_block(
            catchment.nitrogen_total_kg_yr,
            catchment.phosphorus_total_kg_yr,
        ),
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

    edps = [
        _edp_entry(result.spatial.wwtw_id, catchment)
        for catchment in result.catchment_impacts
    ]

    totals = _impact_block(
        result.total.nitrogen_total_kg_yr, result.total.phosphorus_total_kg_yr
    )
    return {
        "edps": edps,
        "totalNitrogen": totals["nitrogenTotal"],
        "totalPhosphorus": totals["phosphorusTotal"],
    }
