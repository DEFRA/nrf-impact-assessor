"""Map assessment results to nrf-backend PATCH /quotes/{reference} payload."""

import logging

from app.clients.bands import get_band
from app.models.domain import CatchmentImpact, ImpactAssessmentResult
from app.models.enums import EdpType

logger = logging.getLogger(__name__)


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


def _provisional_edp_id(catchments: list[CatchmentImpact]) -> str:
    """Pick a stable stand-in for the EDP id.

    The EDP boundary layer carries only EDP_Area and EDP_Name — there is no EDP
    identifier to send, but nrf-backend requires an integer `edpId` and dedupes
    on (quote_id, edp_id). Until a real identifier exists we reuse the lowest NN
    catchment OID, which is at least stable for a given development.

    TODO: replace with the real EDP identifier once the source layer carries one.
    """
    ids = [c.catchment_id for c in catchments]
    numeric = [cid for cid in ids if cid.lstrip("-").isdigit()]
    if numeric:
        return min(numeric, key=int)
    return min(ids)


def _edp_entry(label: str, result: ImpactAssessmentResult) -> dict:
    return {
        "edpId": _provisional_edp_id(result.catchment_impacts),
        "edpName": label,
        "edpType": EdpType.NUTRIENT,
        "impact": _impact_block(
            result.total.nitrogen_total_kg_yr,
            result.total.phosphorus_total_kg_yr,
        ),
        # TODO: replace with real levy once finance calculation in place
        "levyGbp": {"min": 999, "max": 999},
    }


def build_quote_patch_payload(
    results: list[ImpactAssessmentResult],
    edp_labels: list[str],
) -> dict:
    """Build the PATCH body for nrf-backend from assessment results.

    One entry per EDP, not per NN catchment. A single EDP spans several NN
    catchments (the Norfolk EDP covers both Broads and Wensum), and the
    per-catchment figures are development-level totals rather than a split, so
    emitting one entry per catchment named the catchment after an EDP field and
    repeated the same totals — which nrf-backend's `getLevyAmount` sums.

    Args:
        results: Assessment results (typically one per development).
        edp_labels: EDP_Name values for the EDPs the boundary intersects,
            taken from the job's `intersectingEdps`.

    Returns:
        Dict matching the nrf-backend PATCH /quotes/{reference} schema.
        Returns {"edps": []} when there are no results, no catchment impacts,
        or the EDPs cannot be identified.
    """
    if not results:
        return {"edps": []}

    result = results[0]
    if not result.catchment_impacts:
        return {"edps": []}

    if not edp_labels:
        logger.error("No intersecting EDPs on the job, cannot name the EDP entry")
        return {"edps": []}

    if len(edp_labels) > 1:
        # The totals are per development, so they cannot be divided between
        # EDPs, and there is no per-EDP id to keep the entries distinct.
        # Sending them all the full figures would over-report and, once the
        # levy calculation is real, over-charge.
        logger.error(
            f"Boundary intersects {len(edp_labels)} EDPs ({', '.join(edp_labels)}); "
            "impacts cannot be attributed per EDP, skipping callback payload"
        )
        return {"edps": []}

    return {"edps": [_edp_entry(edp_labels[0], result)]}
