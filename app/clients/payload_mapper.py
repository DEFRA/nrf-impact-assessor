"""Map assessment results to nrf-backend PATCH /quotes/{reference} payload."""

from app.clients.bands import get_band
from app.models.domain import ImpactAssessmentResult


def build_quote_patch_payload(
    results: list[ImpactAssessmentResult],
) -> dict:
    """Build the PATCH body for nrf-backend from assessment results.

    Derives EDP entries from the assessment results using nn_catchment as
    the EDP name and a placeholder OID/levy until nrf-backend provides
    real EDP metadata.

    Args:
        results: Assessment results (typically one per development).

    Returns:
        Dict matching the nrf-backend PATCH /quotes/{reference} schema.
    """
    if not results:
        return {"edps": []}

    result = results[0]
    n_total = round(result.total.nitrogen_total_kg_yr, 2)
    p_total = round(result.total.phosphorus_total_kg_yr, 2)
    n_band = get_band(n_total)
    p_band = get_band(p_total)

    nn_catchment = result.spatial.nn_catchment
    if not nn_catchment:
        return {"edps": []}

    mapped_edps = [
        {
            "edpId": result.spatial.wwtw_id,
            "edpName": nn_catchment,
            "edpType": "NUTRIENT",
            "impact": {
                "nitrogenTotal": {
                    "amount": n_total,
                    "unit": "mg/I TP",
                    "band": {"min": n_band, "max": n_band},
                },
                "phosphorusTotal": {
                    "amount": p_total,
                    "unit": "mg/I TP",
                    "band": {"min": p_band, "max": p_band},
                },
            },
            "levyGbp": {
                "min": 999,
                "max": 999,
            },
        }
    ]

    return {"edps": mapped_edps}
