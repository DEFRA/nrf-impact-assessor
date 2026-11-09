"""Migration b7d3f9a1c5e2 replaces levy_charges with edps and base_prices: each
EDP's earliest charge becomes its first base price from the EDP start date, an
empty levy_charges is seeded with EDP 1, and the audit table keeps working
without its levy_charges foreign key. Downgrades and upgrades again."""

from collections.abc import Iterator

import pytest
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from tests.integration.conftest import run_alembic, scratch_database

pytestmark = pytest.mark.integration

MIGRATION_DB = "test_nrf_impact_base_prices"
REVISION = "b7d3f9a1c5e2"
PREVIOUS_REVISION = "a8c2e4f6b1d3"
AUDIT_FK = "fk_audit_levy_calculations_levy_charge"


@pytest.fixture
def engine() -> Iterator[Engine]:
    """A database migrated to the revision before the replacement. The seed
    migration has already inserted the EDP 1 charge of 2675.00."""
    with scratch_database(MIGRATION_DB) as engine:
        run_alembic(MIGRATION_DB, "upgrade", PREVIOUS_REVISION)
        yield engine


def _tables(engine: Engine) -> set[str]:
    return set(inspect(engine).get_table_names(schema="public"))


def _base_prices(engine: Engine) -> list[dict]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT id::text, edp_id, base_price::text,"
                " to_char(effective_from AT TIME ZONE 'UTC',"
                " 'YYYY-MM-DD\"T\"HH24:MI') AS effective_from_utc, comment,"
                " user_entra_id,"
                " user_full_name, user_email"
                " FROM public.base_prices ORDER BY edp_id, effective_from"
            )
        )
        return [dict(zip(rows.keys(), row, strict=True)) for row in rows]


def test_the_seeded_charge_becomes_the_first_base_price(engine: Engine):
    with engine.connect() as conn:
        charge_id = conn.execute(
            text("SELECT id::text FROM public.levy_charges WHERE edp_id = 1")
        ).scalar_one()

    run_alembic(MIGRATION_DB, "upgrade", REVISION)

    assert "levy_charges" not in _tables(engine)
    with engine.connect() as conn:
        edp = conn.execute(text("SELECT id, start_date::text FROM public.edps")).one()
    assert tuple(edp) == (1, "2026-01-01")
    [price] = _base_prices(engine)
    # 2026-01-01 is GMT, so UK midnight is 00:00 UTC.
    assert price["id"] == charge_id
    assert price["edp_id"] == 1
    assert price["base_price"] == "2675.0000"
    assert price["effective_from_utc"] == "2026-01-01T00:00"
    assert price["user_entra_id"] == "system"
    assert price["user_full_name"] == "system"
    assert price["user_email"] == "system@nrf.invalid"


def test_only_the_earliest_charge_of_each_edp_is_carried_over(engine: Engine):
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO public.levy_charges (id, edp_id, edp_name,"
                " edp_start_date, charge_valid_from, charge_valid_to,"
                " base_charge_per_unit) VALUES"
                " (gen_random_uuid(), 1, 'Later window', DATE '2026-01-01',"
                " DATE '2028-01-01', DATE '2028-12-31', 3000),"
                " (gen_random_uuid(), 2, 'Summer EDP', DATE '2026-07-01',"
                " DATE '2026-07-01', DATE '2027-06-30', 100.50)"
            )
        )

    run_alembic(MIGRATION_DB, "upgrade", REVISION)

    prices = _base_prices(engine)
    assert [
        (p["edp_id"], p["base_price"], p["effective_from_utc"]) for p in prices
    ] == [
        (1, "2675.0000", "2026-01-01T00:00"),
        # 2026-07-01 is BST, so UK midnight is 23:00 UTC the day before.
        (2, "100.50", "2026-06-30T23:00"),
    ]
    with engine.connect() as conn:
        names = dict(conn.execute(text("SELECT id, name FROM public.edps")).all())
    assert names[2] == "Summer EDP"


def test_an_empty_levy_charges_is_seeded_with_edp_1(engine: Engine):
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM public.levy_charges"))

    run_alembic(MIGRATION_DB, "upgrade", REVISION)

    [price] = _base_prices(engine)
    assert (price["edp_id"], price["base_price"], price["effective_from_utc"]) == (
        1,
        "2675.00",
        "2026-01-01T00:00",
    )


def test_the_audit_table_no_longer_needs_a_levy_charge_row(engine: Engine):
    run_alembic(MIGRATION_DB, "upgrade", REVISION)

    foreign_keys = inspect(engine).get_foreign_keys(
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
    assert tuple(charge) == (1, "2675.0000", "2026-01-01")
    foreign_keys = inspect(engine).get_foreign_keys(
        "audit_levy_calculations", schema="public"
    )
    assert AUDIT_FK in {fk["name"] for fk in foreign_keys}

    run_alembic(MIGRATION_DB, "upgrade", REVISION)
    assert len(_base_prices(engine)) == 1
