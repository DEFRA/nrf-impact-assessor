"""Lookups and the audit record the levy calculation needs: the EDP id for a
label, the charge row for an EDP on a date, the RICS CIL inflation index for
a charging year, and the audit record of a calculation. Kept out of the
Repository class, which is large and spatial; these are small ORM
operations."""

import logging
from datetime import date

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.calculators.levy import LevyCalculation
from app.data_sync.active_version import get_active_version
from app.models.db import (
    EdpBoundaryLayer,
    LevyCalculationRecord,
    LevyCharge,
    LevyInflationIndex,
)

logger = logging.getLogger(__name__)


def resolve_edp_id(session: Session, label: str) -> int | None:
    """The EDP_id for an EDP label, from the active edp_boundary_layer version.

    The label is the value check-boundary reported (attributes->>'EDP_Name'), so
    the job's label and this key come from the same column. Several polygons can
    share a label, and they must all carry the same EDP_id.

    Returns None when there is no matching row, no EDP_id key, the value is not
    an integer, or the label's polygons disagree on the id. The caller treats
    all of those as "no id" and fails closed.
    """
    version = get_active_version(session, "edp_boundary_layer")
    stmt = (
        select(EdpBoundaryLayer.attributes["EDP_Id"].astext)
        .where(
            EdpBoundaryLayer.version == version,
            EdpBoundaryLayer.attributes["EDP_Name"].astext == label,
        )
        .distinct()
        .limit(2)
    )
    raws = session.execute(stmt).scalars().all()
    if len(raws) > 1:
        # Nothing in the data drop enforces one id per name; picking either
        # could price the quote against another EDP's charge.
        logger.error(f"EDP label {label!r} maps to more than one EDP_id: {raws!r}")
        return None
    if not raws or raws[0] is None:
        return None
    raw = raws[0]
    text = str(raw).strip()
    if not text.isdigit():
        logger.warning(f"EDP_id for label {label!r} is not an integer: {raw!r}")
        return None
    return int(text)


def get_levy_charge(session: Session, edp_id: int, on_date: date) -> LevyCharge | None:
    """The charge row whose validity window contains on_date, or None.

    ex_levy_charges_edp_window stops an EDP's windows overlapping, so at most
    one row can match.
    """
    stmt = select(LevyCharge).where(
        LevyCharge.edp_id == edp_id,
        LevyCharge.charge_valid_from <= on_date,
        LevyCharge.charge_valid_to >= on_date,
    )
    return session.execute(stmt).scalar_one_or_none()


def get_inflation_index(
    session: Session, charging_year: int
) -> LevyInflationIndex | None:
    """The RICS CIL Index row (factor vs the 2026 base) for a charging year.

    Returns the row, not just the factor, so the audit record can name it.
    """
    stmt = select(LevyInflationIndex).where(
        LevyInflationIndex.charging_year == charging_year
    )
    return session.execute(stmt).scalar_one_or_none()


def record_levy_calculation(
    session: Session, quote_reference: str, levy: LevyCalculation
) -> LevyCalculationRecord:
    """Add the scenario 7 audit row for one calculation. The caller commits."""
    row = LevyCalculationRecord(
        quote_reference=quote_reference,
        edp_id=levy.edp_id,
        edp_name=levy.edp_name,
        edp_start_date=levy.edp_start_date,
        calculator_version=levy.calculator_version,
        base_charge_per_unit=levy.base_charge_per_unit,
        units=levy.units,
        calculation_date=levy.calculation_date,
        provisional_amount=levy.provisional_amount,
        inflation_adjusted_amount=levy.inflation_adjusted_amount,
        levy_charge_id=levy.levy_charge_id,
        edp_start_year_index_id=levy.edp_start_year_index_id,
        edp_start_year_index_factor=levy.edp_start_year_index_factor,
        calculation_year_index_id=levy.calculation_year_index_id,
        calculation_year_index_factor=levy.calculation_year_index_factor,
    )
    session.add(row)
    return row
