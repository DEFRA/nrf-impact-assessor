"""Reads and writes of levy_inflation_index rows for the
/inflation-indexes endpoints.

One row per charging year. A row takes effect at 1 January 00:00 UTC of its
charging year and is in force for that whole year; until then it is a
scheduled change an admin can still amend or delete. The index factor is
derived on write from the fixed 2026 base row, never supplied by the caller.

Every add, amendment and deletion also writes a levy_inflation_index_history
row in the same transaction, snapshotting the index as the change saved it —
the change history the admin frontend displays. History rows have no foreign
key to the index, so a deleted index keeps its history.

Each write runs in one transaction that locks the row it depends on. There is
no parent row to serialise adds on (unlike base prices), so concurrent adds
of the same year are settled by uq_levy_inflation_index_year: the losing
flush raises IntegrityError, which is rolled back and reported as
YearAlreadyExistsError. The caller owns the session and its commit.
"""

from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import exists, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models.db import LevyInflationIndex, LevyInflationIndexHistory

# The charging-schedule base the index factor is measured from: its row's
# factor is 1.0000 by definition, and every later factor is
# cil_index / the base row's cil_index.
BASE_CHARGING_YEAR = 2026

_FACTOR_QUANTUM = Decimal("0.0001")


class InflationIndexNotFoundError(Exception):
    """No inflation index has the requested id."""


class YearAlreadyExistsError(Exception):
    """An inflation index already exists for the charging year."""


class InflationIndexTakenEffectError(Exception):
    """The charging year has begun, so the index is now history."""


class ChargingYearNotFutureError(Exception):
    """An index can only be added for a charging year that has not begun."""


class BaseIndexMissingError(Exception):
    """The base charging year's row is missing, so no factor can be derived."""


class InflationIndexInUseError(Exception):
    """The index row has priced a quote, so it cannot be deleted."""


def effective_from(charging_year: int) -> datetime:
    """The moment an index for `charging_year` takes effect: 1 Jan 00:00 UTC."""
    return datetime(charging_year, 1, 1, tzinfo=UTC)


def list_inflation_indexes(session: Session) -> list[LevyInflationIndex]:
    """Every index row, newest charging year first."""
    return (
        session.execute(
            select(LevyInflationIndex).order_by(LevyInflationIndex.charging_year.desc())
        )
        .scalars()
        .all()
    )


def list_inflation_index_history(
    session: Session,
) -> list[LevyInflationIndexHistory]:
    """Every change history row, most recent change first."""
    return (
        session.execute(
            select(LevyInflationIndexHistory).order_by(
                LevyInflationIndexHistory.created_at.desc()
            )
        )
        .scalars()
        .all()
    )


def _record_history(
    session: Session,
    row: LevyInflationIndex,
    action: str,
    *,
    comment: str | None = None,
    staff_id: str | None = None,
    staff_full_name: str | None = None,
    staff_email: str | None = None,
) -> None:
    """Snapshot the index into the history table as the change saved it.

    The row's own staff_* is the last editor, which is who made an added or
    amended change. A delete passes the deleter's details instead, because
    the row is about to go and never knew them.
    """
    session.add(
        LevyInflationIndexHistory(
            inflation_index_id=row.id,
            charging_year=row.charging_year,
            cil_index=row.cil_index,
            index_factor=row.index_factor,
            action=action,
            comment=comment if comment is not None else row.comment,
            requestor=row.requestor,
            jira_ticket=row.jira_ticket,
            staff_id=staff_id or row.staff_id,
            staff_full_name=staff_full_name or row.staff_full_name,
            staff_email=staff_email or row.staff_email,
        )
    )


def add_inflation_index(
    session: Session,
    *,
    charging_year: int,
    cil_index: Decimal,
    comment: str,
    requestor: str,
    jira_ticket: str,
    staff_id: str,
    staff_full_name: str,
    staff_email: str,
    now: datetime,
) -> LevyInflationIndex:
    """Add the index for a charging year that has not begun yet.

    A duplicate year loses the race at the unique constraint; the flush is
    rolled back so the caller's session stays usable.
    """
    if effective_from(charging_year) <= now:
        raise ChargingYearNotFutureError(charging_year)
    year_taken = session.execute(
        select(exists().where(LevyInflationIndex.charging_year == charging_year))
    ).scalar_one()
    if year_taken:
        raise YearAlreadyExistsError(charging_year)
    index_factor = _derive_index_factor(session, cil_index)

    row = LevyInflationIndex(
        charging_year=charging_year,
        cil_index=cil_index,
        index_factor=index_factor,
        comment=comment,
        requestor=requestor,
        jira_ticket=jira_ticket,
        staff_id=staff_id,
        staff_full_name=staff_full_name,
        staff_email=staff_email,
    )
    session.add(row)
    try:
        session.flush()
    except IntegrityError as e:
        session.rollback()
        raise YearAlreadyExistsError(charging_year) from e
    _record_history(session, row, "added")
    return row


def update_inflation_index(
    session: Session, index_id: UUID, changes: dict[str, Any], now: datetime
) -> LevyInflationIndex:
    """Apply changes to an index whose charging year has not begun yet,
    recording the amended state in the history table.

    charging_year cannot change: it is the row's identity, so a different
    year is a different row (delete this one and add that one). The history
    row's staff_* is the row's last editor, so a change that omits the staff
    fields attributes its history to the previous editor.
    """
    row = _lock_future_index(session, index_id, now)
    for field, value in changes.items():
        setattr(row, field, value)
    if "cil_index" in changes:
        row.index_factor = _derive_index_factor(session, row.cil_index)
    session.flush()
    _record_history(session, row, "amended")
    return row


def delete_inflation_index(
    session: Session,
    index_id: UUID,
    now: datetime,
    *,
    comment: str,
    staff_id: str,
    staff_full_name: str,
    staff_email: str,
) -> None:
    """Delete an index whose charging year has not begun yet, recording the
    deletion in the history table with the deleter's details and comment.

    A row that has priced a quote is blocked by the audit table's RESTRICT
    foreign keys; the failed delete is rolled back and reported as
    InflationIndexInUseError.
    """
    row = _lock_future_index(session, index_id, now)
    _record_history(
        session,
        row,
        "deleted",
        comment=comment,
        staff_id=staff_id,
        staff_full_name=staff_full_name,
        staff_email=staff_email,
    )
    session.delete(row)
    try:
        session.flush()
    except IntegrityError as e:
        session.rollback()
        raise InflationIndexInUseError(index_id) from e


def _lock_future_index(
    session: Session, index_id: UUID, now: datetime
) -> LevyInflationIndex:
    row = session.execute(
        select(LevyInflationIndex)
        .where(LevyInflationIndex.id == index_id)
        .with_for_update()
    ).scalar_one_or_none()
    if row is None:
        raise InflationIndexNotFoundError(index_id)
    if effective_from(row.charging_year) <= now:
        raise InflationIndexTakenEffectError(index_id)
    return row


def _derive_index_factor(session: Session, cil_index: Decimal) -> Decimal:
    """The factor relative to the base row's CIL index, at 4 dp half-up."""
    base_cil_index = session.execute(
        select(LevyInflationIndex.cil_index).where(
            LevyInflationIndex.charging_year == BASE_CHARGING_YEAR
        )
    ).scalar_one_or_none()
    if base_cil_index is None:
        raise BaseIndexMissingError(BASE_CHARGING_YEAR)
    return (cil_index / base_cil_index).quantize(
        _FACTOR_QUANTUM, rounding=ROUND_HALF_UP
    )
