"""Migration c9e1a3b5d7f2 adds requestor and jira_ticket to base_prices. The
seeded first price keeps null values; the database refuses a blank requestor
or a ticket that is not NRF2-<number>. Downgrades and upgrades again."""

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

MIGRATION_DB = "test_nrf_impact_base_price_requestor"
TABLE = "base_prices"
REVISION = "c9e1a3b5d7f2"
PREVIOUS_REVISION = "b7d3f9a1c5e2"
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
                "INSERT INTO public.base_prices "
                "(base_price, edp_id, effective_from, comment, requestor, "
                "jira_ticket, staff_id, staff_full_name, staff_email) VALUES "
                "(1, 1, TIMESTAMPTZ '2099-01-01 00:00:00+00', 'c', :requestor, "
                ":jira_ticket, 's', 'n', 'e@example.com')"
            ),
            {"requestor": requestor, "jira_ticket": jira_ticket},
        )


def test_the_seeded_first_price_has_no_requestor_or_ticket(engine: Engine):
    run_alembic(MIGRATION_DB, "upgrade", REVISION)

    with engine.connect() as conn:
        row = conn.execute(
            text(f"SELECT requestor, jira_ticket FROM public.{TABLE}")  # noqa: S608
        ).one()
    assert tuple(row) == (None, None)


def test_accepts_a_requestor_and_a_ticket(engine: Engine):
    run_alembic(MIGRATION_DB, "upgrade", REVISION)

    _insert(engine, requestor="Grace Hopper", jira_ticket="NRF2-1234")

    with engine.connect() as conn:
        count = conn.execute(
            text(
                f"SELECT count(*) FROM public.{TABLE} "  # noqa: S608
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
