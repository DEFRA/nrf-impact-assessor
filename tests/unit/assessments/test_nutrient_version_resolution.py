from unittest.mock import MagicMock

import pytest

from app.assessments.nutrient import NutrientAssessment
from app.models.db import NnCatchments


@pytest.fixture
def assessment():
    repo = MagicMock()
    return NutrientAssessment(rlb_gdf=MagicMock(), metadata={}, repository=repo)


def test_resolve_versions_uses_get_active_version(assessment, monkeypatch):
    """_resolve_versions must resolve the active-version pointer, not raw
    MAX(version), so a rollback (DM-4) actually changes what assessments read.
    """
    calls = []

    def fake_get_active_version(_session, table):
        calls.append(table)
        return 42

    monkeypatch.setattr(
        "app.assessments.nutrient.get_active_version", fake_get_active_version
    )
    # repository.session() is used as a context manager
    assessment.repository.session.return_value.__enter__.return_value = MagicMock()
    assessment.repository.session.return_value.__exit__.return_value = False

    assessment._resolve_versions()

    assert assessment._resolve_latest_version(NnCatchments) == 42
    assert assessment._resolve_latest_coeff_version() == 42
    assert "nn_catchments" in calls
    assert "coefficient_layer" in calls
