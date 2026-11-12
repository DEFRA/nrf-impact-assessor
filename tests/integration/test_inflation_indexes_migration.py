"""Migration 2ddd520a2291 adds the inflation index provenance columns
(comment, staff) with a backfill and the two check constraints, and seeds
the 2026 base row and the 2027 charging-schedule row idempotently.
Downgrades and upgrades again, keeping admin-created rows."""

from collections.abc import Iterator

import pytest
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from tests.integration.conftest import run_alembic, scratch_database

pytestmark = pytest.mark.integration

MIGRATION_DB = "test_nrf_impact_inflation_indexes"
REVISION = "2ddd520a2291"
PREVIOUS_REVISION = "b7d3f9a1c5e2"
BACKFILL_COMMENT = "Seeded with the published RICS CIL Index values"
SCHEDULE_COMMENT = "Factor is 1.0000 throughout 2027, as set by the charging schedule"
PROVENANCE_COLUMNS = ("comment", "staff_id", "staff_full_name", "staff_email")


@pytest.fixture
def engine() -> Iterator[Engine]:
    """A database migrated to the revision before the inflation columns."""
    with scratch_database(MIGRATION_DB) as engine:
        run_alembic(MIGRATION_DB, "upgrade", PREVIOUS_REVISION)
        yield engine


def _indexes(engine: Engine) -> list[dict]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT charging_year, cil_index::text, index_factor::text, "
                "comment, staff_id FROM public.levy_inflation_index "
                "ORDER BY charging_year"
            )
        )
        return [dict(zip(rows.keys(), row, strict=True)) for row in rows]


def _insert_old_shape_row(engine: Engine, charging_year: int) -> str:
    """Insert a pre-migration row (no provenance columns) and return its id."""
    with engine.begin() as conn:
        return conn.execute(
            text(
                "INSERT INTO public.levy_inflation_index "
                "(id, charging_year, cil_index, index_factor) VALUES "
                "(gen_random_uuid(), :year, 400, 1.0000) RETURNING id"
            ),
            {"year": charging_year},
        ).scalar_one()


def test_adds_backfilled_columns_checks_and_seeds(engine: Engine):
    _insert_old_shape_row(engine, 2021)

    run_alembic(MIGRATION_DB, "upgrade", REVISION)

    inspector = inspect(engine)
    columns = {
        c["name"]: c
        for c in inspector.get_columns("levy_inflation_index", schema="public")
    }
    for column in PROVENANCE_COLUMNS:
        assert not columns[column]["nullable"], column
    checks = {
        c["name"]
        for c in inspector.get_check_constraints(
            "levy_inflation_index", schema="public"
        )
    }
    assert {
        "ck_levy_inflation_index_cil_index",
        "ck_levy_inflation_index_comment_not_blank",
    } <= checks

    rows = _indexes(engine)
    assert [(r["charging_year"], r["cil_index"], r["index_factor"]) for r in rows] == [
        (2021, "400.0000", "1.0000"),
        (2026, "400.0000", "1.0000"),
        (2027, "400.0000", "1.0000"),
    ]
    # The pre-existing row is backfilled; the seeds record the system staff.
    assert rows[0]["comment"] == BACKFILL_COMMENT
    assert rows[1]["comment"] == BACKFILL_COMMENT
    assert rows[2]["comment"] == SCHEDULE_COMMENT
    assert {r["staff_id"] for r in rows} == {"system"}


def test_seeding_leaves_an_existing_year_row_untouched(engine: Engine):
    inserted_id = _insert_old_shape_row(engine, 2026)

    run_alembic(MIGRATION_DB, "upgrade", REVISION)

    with engine.connect() as conn:
        kept_id = conn.execute(
            text(
                "SELECT id FROM public.levy_inflation_index WHERE charging_year = 2026"
            )
        ).scalar_one()
    rows = _indexes(engine)
    assert [r["charging_year"] for r in rows] == [2026, 2027]
    assert kept_id == inserted_id  # ON CONFLICT DO NOTHING kept the row
    assert rows[0]["comment"] == BACKFILL_COMMENT


def test_downgrade_keeps_admin_rows_and_upgrades_again(engine: Engine):
    run_alembic(MIGRATION_DB, "upgrade", REVISION)
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO public.levy_inflation_index "
                "(id, charging_year, cil_index, index_factor, comment, "
                "staff_id, staff_full_name, staff_email) VALUES "
                "(gen_random_uuid(), 2099, 410, 1.025, 'NE advice', "
                "'entra-oid', 'Ada Admin', 'ada@example.com')"
            )
        )

    run_alembic(MIGRATION_DB, "downgrade", PREVIOUS_REVISION)

    inspector = inspect(engine)
    column_names = {
        c["name"]
        for c in inspector.get_columns("levy_inflation_index", schema="public")
    }
    assert set(PROVENANCE_COLUMNS).isdisjoint(column_names)
    with engine.connect() as conn:
        years = (
            conn.execute(text("SELECT charging_year FROM public.levy_inflation_index"))
            .scalars()
            .all()
        )
    # The migration's seeds go; the admin row stays.
    assert years == [2099]

    run_alembic(MIGRATION_DB, "upgrade", REVISION)

    assert [r["charging_year"] for r in _indexes(engine)] == [2026, 2027, 2099]
