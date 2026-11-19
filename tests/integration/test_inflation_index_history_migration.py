"""Migration a3f6c8d2e4b7 adds levy_inflation_index_history: one row per
inflation index addition. The table starts empty (the seeded rows predate
admin additions, so there is nothing to backfill), keeps no foreign key to
the index, and its comment check constraint rejects a blank comment."""

from collections.abc import Iterator

import pytest
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

from tests.integration.conftest import run_alembic, scratch_database

pytestmark = pytest.mark.integration

MIGRATION_DB = "test_nrf_impact_inflation_index_history"
REVISION = "a3f6c8d2e4b7"
PREVIOUS_REVISION = "e05a352f9ebe"

INSERT_ROW = (
    "INSERT INTO public.levy_inflation_index_history "
    "(id, inflation_index_id, charging_year, cil_index, "
    "index_factor, comment, staff_id, staff_full_name, "
    "staff_email) VALUES (gen_random_uuid(), "
    "gen_random_uuid(), 2028, 384.21, 0.9605, "
    ":comment, 'entra-oid', 'Ada Admin', 'ada@example.com')"
)


@pytest.fixture
def engine() -> Iterator[Engine]:
    """A database migrated to the revision before the history table."""
    with scratch_database(MIGRATION_DB) as engine:
        run_alembic(MIGRATION_DB, "upgrade", PREVIOUS_REVISION)
        yield engine


def test_adds_the_history_table_with_its_checks(engine: Engine):
    run_alembic(MIGRATION_DB, "upgrade", REVISION)

    inspector = inspect(engine)
    columns = {
        c["name"]: c
        for c in inspector.get_columns("levy_inflation_index_history", schema="public")
    }
    assert set(columns) == {
        "id",
        "inflation_index_id",
        "charging_year",
        "cil_index",
        "index_factor",
        "comment",
        "staff_id",
        "staff_full_name",
        "staff_email",
        "requestor",
        "jira_ticket",
        "created_at",
    }
    # requestor and jira_ticket are nullable: they are null on an index row
    # that predates those columns, and the history snapshots the row as-is.
    assert columns["requestor"]["nullable"]
    assert columns["jira_ticket"]["nullable"]
    assert not columns["comment"]["nullable"]
    assert not columns["inflation_index_id"]["nullable"]
    checks = {
        c["name"]
        for c in inspector.get_check_constraints(
            "levy_inflation_index_history", schema="public"
        )
    }
    assert "ck_levy_inflation_index_history_comment_not_blank" in checks
    indexes = {
        i["name"]
        for i in inspector.get_indexes("levy_inflation_index_history", schema="public")
    }
    assert "ix_public_levy_inflation_index_history_inflation_index_id" in indexes


def test_accepts_an_addition_row_and_rejects_a_blank_comment(engine: Engine):
    run_alembic(MIGRATION_DB, "upgrade", REVISION)

    with engine.begin() as conn:
        conn.execute(text(INSERT_ROW), {"comment": "NE advice"})

    with (
        pytest.raises(
            IntegrityError, match="ck_levy_inflation_index_history_comment_not_blank"
        ),
        engine.begin() as conn,
    ):
        conn.execute(text(INSERT_ROW), {"comment": "   "})


def test_downgrade_drops_the_table(engine: Engine):
    run_alembic(MIGRATION_DB, "upgrade", REVISION)

    run_alembic(MIGRATION_DB, "downgrade", PREVIOUS_REVISION)

    inspector = inspect(engine)
    assert not inspector.has_table("levy_inflation_index_history", schema="public")
