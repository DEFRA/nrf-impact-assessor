"""Map assessment results to nrf-backend PATCH /quotes/{reference} payload."""

import logging

from app.calculators.levy import LevyCalculation
from app.clients.bands import get_band
from app.models.domain import ImpactAssessmentResult
from app.models.enums import EdpType
from app.models.job import IntersectingEdp

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


def _levy_block(levy: LevyCalculation) -> dict:
    """The four fields nrf-backend's patchSchema accepts.

    amountExcludingVat and baseAmount are both the provisional amount: the
    backend prints the first in the quote email and the second on the admin
    page as the provisional levy. VAT and the 2028 inflation uplift are not in
    scope (NRF2-913).
    """
    return {
        "amountExcludingVat": float(levy.provisional_amount),
        "amountInflationAdjusted": float(levy.inflation_adjusted_amount),
        "baseAmount": float(levy.provisional_amount),
        "modelVersion": levy.calculator_version,
    }


def _edp_entry(
    job_edp: IntersectingEdp,
    result: ImpactAssessmentResult,
    catchments: list[dict],
    levy: LevyCalculation,
) -> dict:
    return {
        "edpId": levy.edp_id,
        "edpName": job_edp.label,
        "edpType": EdpType.NUTRIENT,
        "impact": _impact_block(
            result.total.nitrogen_total_kg_yr,
            result.total.phosphorus_total_kg_yr,
        ),
        "catchments": catchments,
        "levyGbp": _levy_block(levy),
    }


def build_quote_patch_payload(
    results: list[ImpactAssessmentResult],
    intersecting_edps: list[IntersectingEdp],
    catchments: list[dict] | None = None,
    *,
    levy: LevyCalculation,
) -> dict | None:
    """Build the PATCH body for nrf-backend from assessment results.

    One entry per EDP, not per NN catchment. A single EDP spans several NN
    catchments (the Norfolk EDP covers both Broads and Wensum), and the
    per-catchment figures are development-level totals rather than a split, so
    emitting one entry per catchment put a catchment name in an EDP field and
    repeated the same totals — which nrf-backend's `getLevyAmount` then sums.

    Args:
        results: Assessment results (typically one per development).
        intersecting_edps: The EDPs the boundary intersects, taken from the
            job's `intersectingEdps`.
        catchments: The boundary's NN catchments, as computed by
            find_intersecting_catchments against the same data version the
            assessment ran on.
        levy: The levy calculated for the single intersecting EDP; carries the
            resolved EDP_id used as `edpId`.

    Returns:
        Dict matching the nrf-backend PATCH /quotes/{reference} schema, or None
        when there is nothing to send: no results, no catchment impacts, or
        the EDPs cannot be identified. The caller must not PATCH then.
    """
    if not results:
        return None

    result = results[0]
    if not result.catchment_impacts:
        return None

    if not intersecting_edps:
        logger.error("No intersecting EDPs on the job, cannot name the EDP entry")
        return None

    if len(intersecting_edps) > 1:
        # The totals are per development, so they cannot be divided between
        # EDPs. Sending them all the full figures would over-charge.
        labels = ", ".join(edp.label for edp in intersecting_edps)
        logger.error(
            f"Boundary intersects {len(intersecting_edps)} EDPs ({labels}); "
            "impacts cannot be attributed per EDP, skipping callback payload"
        )
        return None

    return {"edps": [_edp_entry(intersecting_edps[0], result, catchments or [], levy)]}
