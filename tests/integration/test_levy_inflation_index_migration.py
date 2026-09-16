"""The levy_inflation_index migration creates the table and seeds the
published RICS CIL Index factors (vs the 2026 base) for 2021-2026."""

import importlib.util
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import select, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.models.db import LevyInflationIndex

pytestmark = pytest.mark.integration

_MIGRATION = (
    Path(__file__).parents[2]
    / "alembic/versions/d4e8f1a2b3c5_add_levy_charges_and_calculations.py"
)
_spec = importlib.util.spec_from_file_location("levy_migration", _MIGRATION)
levy_index_migration = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(levy_index_migration)


@pytest.fixture
def reseeded_engine(test_engine: Engine) -> Engine:
    """Re-apply the migration seed so this test is independent of truncation
    done by other integration tests."""
    with test_engine.begin() as conn:
        conn.execute(text("DELETE FROM public.levy_inflation_index"))
        levy_index_migration.seed_index(conn)
    return test_engine


def test_migration_seeds_published_index_factors(reseeded_engine: Engine):
    with Session(reseeded_engine) as session:
        rows = session.scalars(select(LevyInflationIndex)).all()

    by_year = {row.charging_year: row.index_factor for row in rows}
    assert by_year == {
        2021: Decimal("0.8325"),
        2022: Decimal("0.8300"),
        2023: Decimal("0.8875"),
        2024: Decimal("0.9525"),
        2025: Decimal("0.9775"),
        2026: Decimal("1.0000"),
    }
