"""Regression test fixtures.

Fixtures for regression tests that use the full production database.
"""

from pathlib import Path

import pytest
from sqlalchemy import create_engine

from app.repositories.repository import Repository


@pytest.fixture
def test_data_dir() -> Path:
    """Path to test data directory.

    Returns:
        Path to tests/data directory
    """
    return Path(__file__).parent.parent / "data"


@pytest.fixture
def production_repository() -> Repository:
    """Repository connected to production nrf_impact database.

    Requires:
    - PostgreSQL running with PostGIS
    - Database migrations applied
    - Full reference data loaded via scripts/load_data.py

    Returns:
        Repository instance connected to nrf_impact database
    """
    engine = create_engine("postgresql://postgres:password@localhost:5432/nrf_impact")
    return Repository(engine)


@pytest.fixture
def tolerance() -> dict[str, float]:
    """Numerical tolerance for comparing outputs.

    Returns:
        Dictionary of tolerances for different value types
    """
    return {
        "absolute": 0.01,  # kg/year - allow 0.01 kg difference
        "relative": 0.02,  # 2% relative difference (PostGIS ST_Area vs Python geometry.area)
    }
