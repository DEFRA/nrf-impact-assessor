"""Lookups the levy calculation needs: the EDP id for a label, and the base
price and RICS CIL inflation index rows that price an EDP at a moment. Kept out of the
Repository class, which is large and spatial; these are small ORM
operations."""

import logging
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import NamedTuple

from sqlalchemy import func, select
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
    calculation carries, when it took effect, and the EDP's original (oldest)
    base price."""

    edp_id: int
    edp_start_date: date
    base_price_per_unit: Decimal
    base_price_effective_from: datetime
    original_base_price_per_unit: Decimal


class LevyPricingRows(NamedTuple):
    """The charge and the RICS CIL index rows from its effective year to the
    calculation year, ascending. A year with no row is absent."""

    charge: BasePriceInForce
    indices: list[LevyInflationIndex]


def get_levy_pricing_rows(
    session: Session, edp_id: int, at: datetime
) -> LevyPricingRows | None:
    """The EDP's base price in force at `at` (its latest row with
    effective_from at or before `at`), the EDP's oldest base price, and the
    index rows for each year from that row's effective year to `at`'s year.
    None when the EDP does not exist or has no base price in force yet. `at`
    must be timezone-aware and in UTC.

    One statement, so both come from one snapshot: two reads could pair an
    older base price with an index saved after it, a state never committed.
    """
    in_force = (
        select(BasePrice)
        .where(BasePrice.edp_id == edp_id, BasePrice.effective_from <= at)
        .order_by(BasePrice.effective_from.desc())
        .limit(1)
        .subquery()
    )
    base_price = aliased(BasePrice, in_force)
    base_year = func.extract("year", func.timezone("UTC", base_price.effective_from))
    index = aliased(LevyInflationIndex)
    oldest_price = aliased(BasePrice)
    original_price = (
        select(oldest_price.base_price)
        .where(oldest_price.edp_id == edp_id)
        .order_by(oldest_price.effective_from.asc(), oldest_price.id.asc())
        .limit(1)
        .scalar_subquery()
    )
    stmt = (
        select(base_price, Edp, original_price, index)
        .join(Edp, Edp.id == base_price.edp_id)
        .outerjoin(index, index.charging_year.between(base_year, at.year))
        .order_by(index.charging_year)
    )
    rows = session.execute(stmt).all()
    if not rows:
        return None
    price, edp, original, _ = rows[0]
    charge = BasePriceInForce(
        edp_id=edp.id,
        edp_start_date=edp.start_date,
        base_price_per_unit=price.base_price,
        base_price_effective_from=price.effective_from.astimezone(UTC),
        original_base_price_per_unit=original,
    )
    return LevyPricingRows(charge, [index for *_, index in rows if index is not None])
