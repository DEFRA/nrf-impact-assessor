"""Pre-flight guard: a nutrient assessment must not run against empty
reference tables, otherwise it silently produces meaningless results."""

from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

from app.assessments.reference_data import (
    EmptyReferenceDataError,
    assert_reference_data_present,
)


def _repository(counts: dict[str, int]) -> MagicMock:
    """Build a repository whose session counts each table per `counts`,
    keyed by the model's __tablename__."""
    session = MagicMock()

    def _scalar(stmt):
        # The compiled count selects FROM a single table; pull its name out.
        table = stmt.get_final_froms()[0].name
        return counts.get(table, 0)

    session.scalar.side_effect = _scalar

    @contextmanager
    def _session():
        yield session

    repo = MagicMock()
    repo.session.side_effect = _session
    return repo


_ALL_PRESENT = {
    "coefficient_layer": 5,
    "lookup_table": 5,
    "wwtw_catchments": 5,
    "lpa_boundaries": 5,
    "nn_catchments": 5,
    "subcatchments": 5,
}


def test_passes_when_all_required_tables_have_rows():
    repo = _repository(_ALL_PRESENT)
    assert_reference_data_present(repo, "nutrient")  # must not raise


def test_raises_naming_the_empty_table():
    counts = dict(_ALL_PRESENT, wwtw_catchments=0)
    repo = _repository(counts)

    with pytest.raises(EmptyReferenceDataError) as exc:
        assert_reference_data_present(repo, "nutrient")

    assert "wwtw_catchments" in str(exc.value)
    assert "coefficient_layer" not in str(exc.value)


def test_raises_naming_every_empty_table():
    counts = dict(_ALL_PRESENT, wwtw_catchments=0, lookup_table=0)
    repo = _repository(counts)

    with pytest.raises(EmptyReferenceDataError) as exc:
        assert_reference_data_present(repo, "nutrient")

    assert "wwtw_catchments" in str(exc.value)
    assert "lookup_table" in str(exc.value)


def test_unknown_assessment_type_is_not_guarded():
    # No required-table mapping → nothing to assert, must not raise.
    repo = _repository({})
    assert_reference_data_present(repo, "gcn")
