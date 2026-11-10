"""Lookups and the audit record the levy calculation needs: the EDP id for a
label, the base price and RICS CIL inflation index rows that price an EDP at
a moment, and the audit record of a calculation. Kept out of the
Repository class, which is large and spatial; these are small ORM
operations."""

import logging
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import NamedTuple
from uuid import UUID

from sqlalchemy import Integer, cast, extract, func, select, update
from sqlalchemy.orm import Session, aliased

from app.calculators.levy import LevyCalculation
from app.data_sync.active_version import get_active_version
from app.models.db import (
    BasePrice,
    Edp,
    EdpBoundaryLayer,
    LevyCalculationRecord,
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


class BasePriceInForce(NamedTuple):
    """The base price in force for an EDP, with the EDP fields the levy
    calculation and its audit record carry. id is the base_prices row's id."""

    id: int
    edp_id: int
    edp_name: str
    edp_start_date: date
    base_charge_per_unit: Decimal


class LevyPricingRows(NamedTuple):
    """The charge and the RICS CIL index rows that price a levy."""

    charge: BasePriceInForce
    edp_start_year_index: LevyInflationIndex | None
    calculation_year_index: LevyInflationIndex | None


def get_levy_pricing_rows(
    session: Session, edp_id: int, at: datetime
) -> LevyPricingRows | None:
    """The EDP's base price in force at `at` (its latest row with
    effective_from at or before `at`) with the index rows for its start year
    and for the UTC year `at` falls in, in one round trip. None when the EDP
    does not exist or has no base price in force yet.

    `at` must be timezone-aware. Either index is None when that year has none.
    Both are fetched even in the EDP start year, where calculate_levy ignores
    them; a lookup by the unique charging_year costs less than a second query.
    """
    start_index = aliased(LevyInflationIndex)
    calculation_index = aliased(LevyInflationIndex)
    stmt = (
        select(BasePrice, Edp, start_index, calculation_index)
        .join(Edp, Edp.id == BasePrice.edp_id)
        .outerjoin(
            start_index,
            start_index.charging_year == cast(extract("year", Edp.start_date), Integer),
        )
        .outerjoin(
            calculation_index,
            calculation_index.charging_year == at.astimezone(UTC).year,
        )
        .where(BasePrice.edp_id == edp_id, BasePrice.effective_from <= at)
        .order_by(BasePrice.effective_from.desc())
        .limit(1)
    )
    row = session.execute(stmt).one_or_none()
    if row is None:
        return None
    base_price, edp, start, calculation = row
    charge = BasePriceInForce(
        id=base_price.id,
        edp_id=edp.id,
        edp_name=edp.name,
        edp_start_date=edp.start_date,
        base_charge_per_unit=base_price.base_price,
    )
    return LevyPricingRows(charge, start, calculation)


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


def mark_levy_calculation_sent(session: Session, audit_id: UUID) -> None:
    """Stamp sent_at on the audit row a delivered quote was priced from.

    By id, not quote reference: a redelivered job records one row per
    attempt, and only the one that was sent is stamped. The caller commits.
    """
    session.execute(
        update(LevyCalculationRecord)
        .where(LevyCalculationRecord.id == audit_id)
        .values(sent_at=func.now())
    )
