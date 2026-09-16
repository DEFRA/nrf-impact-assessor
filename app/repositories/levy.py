"""Lookups and the audit record the levy calculation needs: the EDP id for a
label, the charge row for an EDP on a date, the RICS CIL inflation index for
a charging year, and the audit record of a calculation. Kept out of the
Repository class, which is large and spatial; these are small ORM
operations."""

import logging
from datetime import date
from decimal import Decimal

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
    share a label; EDP_id is unique per EDP so any matching row will do.

    Returns None when there is no matching row, no EDP_id key, or the value is
    not an integer. The caller treats all of those as "no id" and fails closed.
    """
    version = get_active_version(session, "edp_boundary_layer")
    stmt = (
        select(EdpBoundaryLayer.attributes["EDP_Id"].astext)
        .where(
            EdpBoundaryLayer.version == version,
            EdpBoundaryLayer.attributes["EDP_Name"].astext == label,
        )
        # EDP_id is expected to be unique per EDP, but nothing in the schema
        # enforces it. Ordering makes the answer stable if a data drop ever
        # puts two ids under one name, so a quote cannot be priced from a
        # different id on a retry than on its first attempt.
        .order_by(EdpBoundaryLayer.attributes["EDP_Id"].astext)
        .limit(1)
    )
    raw = session.execute(stmt).scalar_one_or_none()
    if raw is None:
        return None
    text = str(raw).strip()
    if not text.isdigit():
        logger.warning(f"EDP_id for label {label!r} is not an integer: {raw!r}")
        return None
    return int(text)


def get_levy_charge(session: Session, edp_id: int, on_date: date) -> LevyCharge | None:
    """The charge row whose validity window contains on_date, or None."""
    stmt = (
        select(LevyCharge)
        .where(
            LevyCharge.edp_id == edp_id,
            LevyCharge.charge_valid_from <= on_date,
            LevyCharge.charge_valid_to >= on_date,
        )
        .order_by(LevyCharge.charge_valid_from.desc())
        .limit(1)
    )
    return session.execute(stmt).scalar_one_or_none()


def get_inflation_index(session: Session, charging_year: int) -> Decimal | None:
    """The RICS CIL Index factor (vs the 2026 base) for a charging year, or None."""
    stmt = select(LevyInflationIndex.index_factor).where(
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
        rounded_charge_per_unit=levy.rounded_charge_per_unit,
        units=levy.units,
        calculation_date=levy.calculation_date,
        provisional_amount=levy.provisional_amount,
        inflation_adjusted_amount=levy.inflation_adjusted_amount,
    )
    session.add(row)
    return row
