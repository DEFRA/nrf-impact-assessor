"""Lookups the levy calculation needs: the EDP id for a
label, and the base price and RICS CIL inflation index rows that price an EDP at
a moment. Kept out of the
Repository class, which is large and spatial; these are small ORM
operations."""

import logging
from datetime import date, datetime
from decimal import Decimal
from typing import NamedTuple

from sqlalchemy import Integer, cast, extract, select
from sqlalchemy.orm import Session, aliased

from app.data_sync.active_version import get_active_version
from app.models.db import (
    BasePrice,
    Edp,
    EdpBoundaryLayer,
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
    calculation carries, and its original (oldest) base price."""

    edp_id: int
    edp_start_date: date
    base_charge_per_unit: Decimal
    original_base_charge_per_unit: Decimal


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
    and for the year `at` falls in, in one round trip. None when the EDP
    does not exist or has no base price in force yet.

    `at` must be timezone-aware and in UTC. Either index is None when that year has none.
    Both are fetched even in the EDP start year, where calculate_levy ignores
    them; a lookup by the unique charging_year costs less than a second query.
    """
    start_index = aliased(LevyInflationIndex)
    calculation_index = aliased(LevyInflationIndex)
    original_price = (
        select(BasePrice.base_price)
        .where(BasePrice.edp_id == edp_id)
        .order_by(BasePrice.effective_from.asc())
        .limit(1)
        .scalar_subquery()
    )
    stmt = (
        select(BasePrice, Edp, original_price, start_index, calculation_index)
        .join(Edp, Edp.id == BasePrice.edp_id)
        .outerjoin(
            start_index,
            start_index.charging_year == cast(extract("year", Edp.start_date), Integer),
        )
        .outerjoin(
            calculation_index,
            calculation_index.charging_year == at.year,
        )
        .where(BasePrice.edp_id == edp_id, BasePrice.effective_from <= at)
        .order_by(BasePrice.effective_from.desc())
        .limit(1)
    )
    row = session.execute(stmt).one_or_none()
    if row is None:
        return None
    base_price, edp, original, start, calculation = row
    charge = BasePriceInForce(
        edp_id=edp.id,
        edp_start_date=edp.start_date,
        base_charge_per_unit=base_price.base_price,
        original_base_charge_per_unit=original,
    )
    return LevyPricingRows(charge, start, calculation)
