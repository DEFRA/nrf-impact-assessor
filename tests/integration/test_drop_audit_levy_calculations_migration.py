"""Downgrade of 9a4d7c2b1e30 recreates audit_levy_calculations empty, with the
columns, foreign keys and indexes it had before the drop."""

from collections.abc import Iterator

import pytest
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from tests.integration.conftest import run_alembic, scratch_database

pytestmark = pytest.mark.integration

MIGRATION_DB = "test_nrf_impact_drop_audit_levy_calculations"
TABLE = "audit_levy_calculations"
REVISION = "9a4d7c2b1e30"
PREVIOUS_REVISION = "a3f6c8d2e4b7"


@pytest.fixture
def engine() -> Iterator[Engine]:
    with scratch_database(MIGRATION_DB) as engine:
        run_alembic(MIGRATION_DB, "upgrade", PREVIOUS_REVISION)
        yield engine


def _has_table(engine: Engine) -> bool:
    return inspect(engine).has_table(TABLE, schema="public")


def test_downgrade_recreates_the_table_empty(engine: Engine):
    before = inspect(engine)
    columns = {c["name"]: str(c["type"]) for c in before.get_columns(TABLE, "public")}
    foreign_keys = {fk["name"] for fk in before.get_foreign_keys(TABLE, "public")}
    indexes = {ix["name"] for ix in before.get_indexes(TABLE, "public")}

    run_alembic(MIGRATION_DB, "upgrade", REVISION)
    assert not _has_table(engine)
    run_alembic(MIGRATION_DB, "downgrade", PREVIOUS_REVISION)

    after = inspect(engine)
    assert {
        c["name"]: str(c["type"]) for c in after.get_columns(TABLE, "public")
    } == columns
    assert {fk["name"] for fk in after.get_foreign_keys(TABLE, "public")} == (
        foreign_keys
    )
    assert {ix["name"] for ix in after.get_indexes(TABLE, "public")} == indexes
    with engine.connect() as conn:
        count = conn.execute(
            text("SELECT count(*) FROM public.audit_levy_calculations")
        )
        assert count.scalar_one() == 0
