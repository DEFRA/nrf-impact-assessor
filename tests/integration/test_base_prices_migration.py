"""Migration b7d3f9a1c5e2 replaces levy_charges with edps and base_prices: EDP 1
is seeded with its first base price, and the audit table's levy_charge_id
becomes an integer without its levy_charges foreign key. Downgrades and
upgrades again."""

from collections.abc import Iterator

import pytest
from sqlalchemy import Integer, inspect, text
from sqlalchemy.engine import Engine

from tests.integration.conftest import run_alembic, scratch_database

pytestmark = pytest.mark.integration

MIGRATION_DB = "test_nrf_impact_base_prices"
REVISION = "b7d3f9a1c5e2"
PREVIOUS_REVISION = "a8c2e4f6b1d3"
AUDIT_FK = "fk_audit_levy_calculations_levy_charge"


@pytest.fixture
def engine() -> Iterator[Engine]:
    """A database migrated to the revision before the replacement."""
    with scratch_database(MIGRATION_DB) as engine:
        run_alembic(MIGRATION_DB, "upgrade", PREVIOUS_REVISION)
        yield engine


def _tables(engine: Engine) -> set[str]:
    return set(inspect(engine).get_table_names(schema="public"))


def _base_prices(engine: Engine) -> list[dict]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT id, edp_id, base_price::text,"
                " to_char(effective_from AT TIME ZONE 'UTC',"
                " 'YYYY-MM-DD\"T\"HH24:MI') AS effective_from_utc, comment,"
                " user_entra_id,"
                " user_full_name, user_email"
                " FROM public.base_prices ORDER BY edp_id, effective_from"
            )
        )
        return [dict(zip(rows.keys(), row, strict=True)) for row in rows]


def test_edp_1_is_seeded_with_its_first_base_price(engine: Engine):
    run_alembic(MIGRATION_DB, "upgrade", REVISION)

    assert "levy_charges" not in _tables(engine)
    with engine.connect() as conn:
        edp = conn.execute(text("SELECT id, start_date::text FROM public.edps")).one()
    assert tuple(edp) == (1, "2026-01-01")
    [price] = _base_prices(engine)
    assert price["id"] == 1
    assert price["edp_id"] == 1
    assert price["base_price"] == "2675.00"
    assert price["effective_from_utc"] == "2026-01-01T00:00"
    assert price["user_entra_id"] == "system"
    assert price["user_full_name"] == "system"
    assert price["user_email"] == "system@nrf.invalid"


def test_the_audit_table_holds_integer_base_price_ids_without_a_foreign_key(
    engine: Engine,
):
    run_alembic(MIGRATION_DB, "upgrade", REVISION)

    inspector = inspect(engine)
    columns = {
        c["name"]: c["type"]
        for c in inspector.get_columns("audit_levy_calculations", schema="public")
    }
    assert isinstance(columns["levy_charge_id"], Integer)
    foreign_keys = inspector.get_foreign_keys(
        "audit_levy_calculations", schema="public"
    )
    assert AUDIT_FK not in {fk["name"] for fk in foreign_keys}


def test_downgrade_and_upgrade_again(engine: Engine):
    run_alembic(MIGRATION_DB, "upgrade", REVISION)

    run_alembic(MIGRATION_DB, "downgrade", PREVIOUS_REVISION)
    assert {"edps", "base_prices"}.isdisjoint(_tables(engine))
    with engine.connect() as conn:
        charge = conn.execute(
            text(
                "SELECT edp_id, base_charge_per_unit::text, charge_valid_from::text"
                " FROM public.levy_charges"
            )
        ).one()
    assert tuple(charge) == (1, "2675.00", "2026-01-01")
    foreign_keys = inspect(engine).get_foreign_keys(
        "audit_levy_calculations", schema="public"
    )
    assert AUDIT_FK in {fk["name"] for fk in foreign_keys}

    run_alembic(MIGRATION_DB, "upgrade", REVISION)
    assert len(_base_prices(engine)) == 1
