"""Levy pricing shared by the quote path and the recalculation endpoint, so
both apply the same charge and inflation rules."""

from datetime import date

from sqlalchemy.orm import Session

from app.calculators.levy import LevyCalculation, calculate_levy
from app.repositories.levy import get_levy_pricing_rows


def price_levy(
    session: Session,
    edp_id: int,
    units: int,
    calculation_date: date,
    *,
    apply_inflation: bool = True,
) -> LevyCalculation | None:
    """Price the levy for an EDP on a date from its charge and the RICS CIL
    indices. Returns None when the EDP has no charge covering the date.

    The charge and both indices come from one query; calculate_levy applies
    the indices only when calculation_date falls in a later charging year
    than the EDP's publication, unless apply_inflation is False.

    Raises:
        LevyChargeUnavailableError: An inflation index needed is missing, or the
            charge is not a whole number of pounds.
    """
    rows = get_levy_pricing_rows(session, edp_id, calculation_date)
    if rows is None:
        return None

    return calculate_levy(
        rows.charge,
        units=units,
        calculation_date=calculation_date,
        edp_start_year_index=rows.edp_start_year_index,
        calculation_year_index=rows.calculation_year_index,
        apply_inflation=apply_inflation,
    )
