"""Levy pricing shared by the quote path and the recalculation endpoint, so
both apply the same charge and inflation rules."""

from datetime import datetime

from sqlalchemy.orm import Session

from app.calculators.levy import LevyCalculation, calculate_levy
from app.repositories.levy import get_levy_pricing_rows


def price_levy(
    session: Session,
    edp_id: int,
    units: int,
    at: datetime,
    *,
    apply_inflation: bool = True,
) -> LevyCalculation | None:
    """Price the levy for an EDP at a moment from the base price in force then
    and the RICS CIL indices. Returns None when the EDP has no base price in
    force at `at`. `at` must be timezone-aware and in UTC.

    The base price and both indices come from one query; calculate_levy applies
    the indices only when the date of `at` falls in a later charging year
    than the EDP's publication, unless apply_inflation is False.

    Raises:
        LevyChargeUnavailableError: apply_inflation is True and an inflation
            index needed is missing, or the levy is too large to record.
    """
    rows = get_levy_pricing_rows(session, edp_id, at)
    if rows is None:
        return None

    return calculate_levy(
        rows.charge,
        units=units,
        calculation_date=at.date(),
        edp_start_year_index=rows.edp_start_year_index,
        calculation_year_index=rows.calculation_year_index,
        apply_inflation=apply_inflation,
    )
