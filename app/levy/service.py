"""Levy pricing shared by the quote path and the recalculation endpoint, so
both apply the same rule."""

from datetime import datetime

from sqlalchemy.orm import Session

from app.calculators.levy import LevyCalculation, calculate_levy
from app.repositories.levy import get_levy_pricing_rows


def price_levy(
    session: Session, edp_id: int, units: int, at: datetime
) -> LevyCalculation | None:
    """Price the levy for an EDP at a moment. Returns None when the EDP has no
    base price in force at `at`. `at` must be timezone-aware and in UTC. A
    levy under MIN_LEVY_AMOUNT is £0, so its index rows are not read.

    Raises:
        LevyChargeUnavailableError: an index is missing or the levy is too large.
    """
    rows = get_levy_pricing_rows(session, edp_id, units, at)
    if rows is None:
        return None
    return calculate_levy(rows.charge, units, at.date(), rows.indices)
