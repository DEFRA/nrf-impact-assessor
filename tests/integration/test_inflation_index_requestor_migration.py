"""Migration e05a352f9ebe adds requestor and jira_ticket to
levy_inflation_index. The seeded rows keep null values; the database refuses
a blank requestor or a ticket that is not NRF2-<number>. Downgrades and
upgrades again."""

from collections.abc import Iterator

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

from tests.integration.conftest import run_alembic, scratch_database
from tests.integration.requestor_migration_checks import (
    INVALID_REQUESTOR_TICKET_PAIRS,
    assert_downgrade_and_upgrade_again,
    assert_insert_is_refused,
)

pytestmark = pytest.mark.integration

MIGRATION_DB = "test_nrf_impact_inflation_index_requestor"
TABLE = "levy_inflation_index"
REVISION = "e05a352f9ebe"
PREVIOUS_REVISION = "2ddd520a2291"
NEW_COLUMNS = {"requestor", "jira_ticket"}


@pytest.fixture
def engine() -> Iterator[Engine]:
    """A database migrated to the revision before the new columns."""
    with scratch_database(MIGRATION_DB) as engine:
        run_alembic(MIGRATION_DB, "upgrade", PREVIOUS_REVISION)
        yield engine


def _insert(engine: Engine, requestor: str | None, jira_ticket: str | None) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO public.levy_inflation_index "
                "(id, charging_year, cil_index, index_factor, comment, requestor, "
                "jira_ticket, staff_id, staff_full_name, staff_email) VALUES "
                "(gen_random_uuid(), 2099, 384.21, 0.9605, 'c', :requestor, "
                ":jira_ticket, 's', 'n', 'e@example.com')"
            ),
            {"requestor": requestor, "jira_ticket": jira_ticket},
        )


def test_the_seeded_rows_have_no_requestor_or_ticket(engine: Engine):
    run_alembic(MIGRATION_DB, "upgrade", REVISION)

    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT requestor, jira_ticket FROM public.levy_inflation_index")
        ).all()
    assert rows
    assert all(tuple(row) == (None, None) for row in rows)


def test_accepts_a_requestor_and_a_ticket(engine: Engine):
    run_alembic(MIGRATION_DB, "upgrade", REVISION)

    _insert(engine, requestor="Grace Hopper", jira_ticket="NRF2-1234")

    with engine.connect() as conn:
        count = conn.execute(
            text(
                "SELECT count(*) FROM public.levy_inflation_index "
                "WHERE jira_ticket IS NOT NULL"
            )
        ).scalar_one()
    assert count == 1


@pytest.mark.parametrize(("requestor", "jira_ticket"), INVALID_REQUESTOR_TICKET_PAIRS)
def test_refuses_a_blank_requestor_or_a_malformed_ticket(
    engine: Engine, requestor: str, jira_ticket: str
):
    run_alembic(MIGRATION_DB, "upgrade", REVISION)

    assert_insert_is_refused(engine, _insert, requestor, jira_ticket)


def test_downgrade_and_upgrade_again(engine: Engine):
    assert_downgrade_and_upgrade_again(
        engine, MIGRATION_DB, TABLE, REVISION, PREVIOUS_REVISION, NEW_COLUMNS
    )
