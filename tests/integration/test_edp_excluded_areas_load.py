"""Loading the committed fixture populates edp_excluded_areas correctly."""

from pathlib import Path

import pytest
from load_data import SpatialDataLoader
from sqlalchemy import func, select

from app.models.db import EdpExcludedAreas
from app.repositories.repository import Repository

pytestmark = pytest.mark.integration

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "data" / "fixtures"
FIXTURE = FIXTURES_DIR / "edp_excluded_areas.gpkg"


def test_fixture_exists():
    assert FIXTURE.exists(), "committed edp_excluded_areas.gpkg fixture is missing"


def test_load_populates_rows_and_name(repository: Repository):
    """Load just this layer from the committed fixtures into the test DB."""
    loader = SpatialDataLoader(repository, fixtures_dir=FIXTURES_DIR)
    loader.load_spatial_layers(layer_types=["edp_excluded_areas"])

    with repository.session() as session:
        count = session.scalar(select(func.count()).select_from(EdpExcludedAreas))
        names = session.scalars(select(EdpExcludedAreas.name)).all()

    assert count == 3
    assert "Yare Broads and Marshes SSSI" in names
