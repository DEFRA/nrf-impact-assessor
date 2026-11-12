"""Reads and writes of EDP base prices for the /base-prices endpoints.

Each write runs in one transaction that locks the row it depends on, so two
concurrent requests cannot both pass a check and then both write. The caller
owns the session and its commit.
"""

from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import exists, select
from sqlalchemy.orm import Session

from app.models.db import BasePrice, Edp


class EdpNotFoundError(Exception):
    """The EDP a base price is added to does not exist."""


class BasePriceNotFoundError(Exception):
    """No base price has the requested id."""


class FutureBasePriceExistsError(Exception):
    """The EDP already has a base price that has not taken effect yet."""


class BasePriceTakenEffectError(Exception):
    """The base price's effective_from has passed, so it is now history."""


class EffectiveFromInPastError(Exception):
    """A base price cannot be given an effective_from that has already passed."""


def list_edps_with_base_prices(session: Session) -> list[tuple[Edp, list[BasePrice]]]:
    """Every EDP with its base prices, newest effective_from first."""
    edps = session.execute(select(Edp).order_by(Edp.id)).scalars().all()
    prices = (
        session.execute(
            select(BasePrice).order_by(
                BasePrice.edp_id, BasePrice.effective_from.desc()
            )
        )
        .scalars()
        .all()
    )
    return [(edp, [p for p in prices if p.edp_id == edp.id]) for edp in edps]


def add_base_price(
    session: Session,
    *,
    edp_id: int,
    base_price: Decimal,
    effective_from: datetime,
    comment: str,
    requestor: str,
    jira_ticket: str,
    staff_id: str,
    staff_full_name: str,
    staff_email: str,
    now: datetime,
) -> BasePrice:
    """Add a base price, unless the EDP already has one yet to take effect.

    The EDP row is locked first, so two concurrent adds serialise on the check.
    """
    locked_edp = session.execute(
        select(Edp.id).where(Edp.id == edp_id).with_for_update()
    ).scalar_one_or_none()
    if locked_edp is None:
        raise EdpNotFoundError(edp_id)
    if effective_from <= now:
        raise EffectiveFromInPastError(effective_from)
    has_future_price = session.execute(
        select(
            exists().where(BasePrice.edp_id == edp_id, BasePrice.effective_from > now)
        )
    ).scalar_one()
    if has_future_price:
        raise FutureBasePriceExistsError(edp_id)

    row = BasePrice(
        edp_id=edp_id,
        base_price=base_price,
        effective_from=effective_from,
        comment=comment,
        requestor=requestor,
        jira_ticket=jira_ticket,
        staff_id=staff_id,
        staff_full_name=staff_full_name,
        staff_email=staff_email,
    )
    session.add(row)
    session.flush()
    return row


def update_base_price(
    session: Session, base_price_id: int, changes: dict[str, Any], now: datetime
) -> BasePrice:
    """Apply changes to a base price that has not taken effect yet."""
    row = _lock_future_base_price(session, base_price_id, now)
    new_effective_from = changes.get("effective_from")
    if new_effective_from is not None and new_effective_from <= now:
        raise EffectiveFromInPastError(new_effective_from)
    for field, value in changes.items():
        setattr(row, field, value)
    session.flush()
    return row


def delete_base_price(session: Session, base_price_id: int, now: datetime) -> None:
    """Delete a base price that has not taken effect yet."""
    row = _lock_future_base_price(session, base_price_id, now)
    session.delete(row)
    session.flush()


def _lock_future_base_price(
    session: Session, base_price_id: int, now: datetime
) -> BasePrice:
    row = session.execute(
        select(BasePrice).where(BasePrice.id == base_price_id).with_for_update()
    ).scalar_one_or_none()
    if row is None:
        raise BasePriceNotFoundError(base_price_id)
    if row.effective_from <= now:
        raise BasePriceTakenEffectError(base_price_id)
    return row
