"""Map assessment results to nrf-backend PATCH /quotes/{reference} payload."""

from app.clients.bands import get_band
from app.models.domain import ImpactAssessmentResult
from app.models.job import EdpInput


def build_quote_patch_payload(
    results: list[ImpactAssessmentResult],
    edps: list[EdpInput],
) -> dict:
    """Build the PATCH body for nrf-backend from assessment results and EDP metadata.

    Each EDP in the input gets the nitrogen/phosphorus totals from the first
    assessment result. The levy amounts are passed through from the EDP input.

    Args:
        results: Assessment results (typically one per development).
        edps: EDP metadata from the SQS job message.

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

    mapped_edps = []
    for edp in edps:
        mapped_edps.append(
            {
                "edpId": edp.edp_id,
                "edpName": edp.edp_name,
                "edpType": edp.edp_type,
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
                    "min": round(edp.levy_gbp.min, 2),
                    "max": round(edp.levy_gbp.max, 2),
                },
            }
        )

    return {"edps": mapped_edps}
