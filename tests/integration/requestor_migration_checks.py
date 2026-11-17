"""Shared checks for the requestor / jira_ticket migrations, one suite per
table (base_prices, levy_inflation_index). Both suites must refuse the same
bad values and survive a downgrade round-trip, so the checks live here rather
than copied between the two files."""

from collections.abc import Callable
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

from tests.integration.conftest import run_alembic

# (requestor, jira_ticket) pairs the check constraints must refuse
INVALID_REQUESTOR_TICKET_PAIRS = [
    ("   ", "NRF2-1234"),
    ("Grace Hopper", "nrf2-1234"),
    ("Grace Hopper", "NRF2-"),
    ("Grace Hopper", "NRF2-12a"),
    ("Grace Hopper", "ABC-1234"),
    ("Grace Hopper", "NRF2-1234 "),
]


def table_columns(engine: Engine, table: str) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = :table"
            ),
            {"table": table},
        )
        return {row[0] for row in rows}


def assert_insert_is_refused(
    engine: Engine,
    insert: Callable[..., Any],
    requestor: str,
    jira_ticket: str,
) -> None:
    with pytest.raises(IntegrityError):
        insert(engine, requestor=requestor, jira_ticket=jira_ticket)


def assert_downgrade_and_upgrade_again(
    engine: Engine,
    migration_db: str,
    table: str,
    revision: str,
    previous_revision: str,
    new_columns: set[str],
) -> None:
    run_alembic(migration_db, "upgrade", revision)
    assert table_columns(engine, table) >= new_columns

    run_alembic(migration_db, "downgrade", previous_revision)
    assert new_columns.isdisjoint(table_columns(engine, table))

    run_alembic(migration_db, "upgrade", revision)
    assert table_columns(engine, table) >= new_columns
