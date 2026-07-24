"""The edp_excluded_areas table is created by migrations and mapped."""

import pytest
from sqlalchemy import inspect
from sqlalchemy.engine import Engine

pytestmark = pytest.mark.integration


def test_table_exists_after_migrations(test_engine: Engine):
    """test_engine has run `alembic upgrade head` against test_nrf_impact."""
    inspector = inspect(test_engine)
    tables = inspector.get_table_names(schema="public")
    assert "edp_excluded_areas" in tables

    columns = {
        c["name"] for c in inspector.get_columns("edp_excluded_areas", schema="public")
    }
    assert {"id", "version", "geometry", "name", "attributes", "created_at"} <= columns
