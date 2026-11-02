"""Migration a8c2e4f6b1d3 adds the levy_charges base charge CHECK: it refuses
to upgrade over a charge that breaks it rather than rounding the charge, and
downgrades and re-upgrades cleanly."""

from collections.abc import Iterator

import pytest
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from tests.integration.conftest import run_alembic, scratch_database

pytestmark = pytest.mark.integration

MIGRATION_DB = "test_nrf_impact_levy_base_charge"
REVISION = "a8c2e4f6b1d3"
PREVIOUS_REVISION = "e5f9a2b3c4d6"
CONSTRAINT = "ck_levy_charges_base_charge_per_unit"


@pytest.fixture
def engine() -> Iterator[Engine]:
    """A database migrated to the revision before the CHECK."""
    with scratch_database(MIGRATION_DB) as engine:
        run_alembic(MIGRATION_DB, "upgrade", PREVIOUS_REVISION)
        yield engine


def _set_seeded_charge(engine: Engine, price: str) -> None:
    """Set the EDP 1 charge that seed migration f7b1c3d5e9a2 inserted."""
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE public.levy_charges"
                " SET base_charge_per_unit = CAST(:price AS numeric)"
                " WHERE edp_id = 1"
            ),
            {"price": price},
        )


def _base_charge_column(engine: Engine) -> str:
    columns = inspect(engine).get_columns("levy_charges", schema="public")
    return next(str(c["type"]) for c in columns if c["name"] == "base_charge_per_unit")


def _constraints(engine: Engine) -> set[str]:
    checks = inspect(engine).get_check_constraints("levy_charges", schema="public")
    return {c["name"] for c in checks}


def test_upgrade_fails_on_a_charge_with_more_than_two_dp(engine: Engine):
    _set_seeded_charge(engine, "2193.6649")

    with pytest.raises(RuntimeError, match=CONSTRAINT):
        run_alembic(MIGRATION_DB, "upgrade", REVISION)

    # The type change rolled back with the failed constraint.
    assert _base_charge_column(engine) == "NUMERIC(12, 4)"
    with engine.connect() as conn:
        stored = conn.execute(
            text("SELECT base_charge_per_unit::text FROM public.levy_charges")
        ).scalar_one()
    assert stored == "2193.6649"


def test_downgrade_and_upgrade_again(engine: Engine):
    # Over the seeded 2675.0000 charge.
    run_alembic(MIGRATION_DB, "upgrade", REVISION)
    assert _base_charge_column(engine) == "NUMERIC"
    assert CONSTRAINT in _constraints(engine)

    run_alembic(MIGRATION_DB, "downgrade", PREVIOUS_REVISION)
    assert _base_charge_column(engine) == "NUMERIC(12, 4)"
    assert CONSTRAINT not in _constraints(engine)

    run_alembic(MIGRATION_DB, "upgrade", REVISION)
    assert CONSTRAINT in _constraints(engine)
