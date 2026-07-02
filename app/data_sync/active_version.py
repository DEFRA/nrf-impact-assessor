"""Active-version pointer for reference tables (DM-4 short-term rollback).

Retention keeps MAX(version) and MAX(version)-1 per table (see
old_version_cleanup_sql in restore.py). Reads that need "the live version"
resolve this pointer instead of computing MAX(version) directly, so a
rollback (moving the pointer back one step) actually changes what reads see.
When no pointer row exists yet for a table (e.g. before the first reload or
rollback after this feature ships), get_active_version falls back to
MAX(version) so behaviour is unchanged until a rollback actually happens.
"""

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.repositories.repository import _assert_safe_identifier


def get_active_version(session: Session, table: str) -> int:
    """Return the version reads should use for `table`."""
    _assert_safe_identifier(table, "table")
    query = f"SELECT COALESCE((SELECT active_version FROM public.data_active_version WHERE table_name = :t), (SELECT MAX(version) FROM public.{table}))"  # noqa: S608,E501
    row = session.execute(text(query), {"t": table}).fetchone()
    return row[0] if row and row[0] is not None else 1


def set_active_version(session: Session, table: str, version: int) -> None:
    """Point `table`'s active version at `version` (upsert)."""
    _assert_safe_identifier(table, "table")
    session.execute(
        text(
            "INSERT INTO public.data_active_version (table_name, active_version, updated_at) "
            "VALUES (:t, :v, now()) "
            "ON CONFLICT (table_name) DO UPDATE SET "
            "active_version = EXCLUDED.active_version, updated_at = now()"
        ),
        {"t": table, "v": version},
    )


def rollback_table(session: Session, table: str) -> tuple[int, int]:
    """Move `table`'s active version back one step.

    Returns (from_version, to_version). Raises ValueError if the version
    below the current active one has no retained rows (nothing to roll back
    to) — this is expected once two rollbacks are attempted in a row, or
    before any prior version was ever retained.
    """
    _assert_safe_identifier(table, "table")
    current = get_active_version(session, table)
    target = current - 1
    exists = None
    if target >= 1:
        exists = session.execute(
            text(f"SELECT 1 FROM public.{table} WHERE version = :v LIMIT 1"),  # noqa: S608
            {"v": target},
        ).fetchone()
    if exists is None:
        msg = (
            f"no retained previous version to roll back to for table {table!r} "
            f"(current active version {current})"
        )
        raise ValueError(msg)
    set_active_version(session, table, target)
    return current, target
