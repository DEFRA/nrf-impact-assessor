"""resolve_edp_id parses attributes->>'EDP_Id' strictly: anything that is not
an integer is 'no id', so the caller fails closed (spec decision 6).
record_levy_calculation maps every scenario 7 field onto the audit row."""

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from app.models.db import LevyCalculationRecord
from app.repositories.levy import (
    get_inflation_index,
    record_levy_calculation,
    resolve_edp_id,
)
from tests.conftest import EDP_NAME, LEVY_CHARGE_ID, make_levy_calculation

LABEL = "Broads SAC EDP"


def _session_returning(value) -> MagicMock:
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = value
    return session


def _session_returning_ids(*values) -> MagicMock:
    session = MagicMock()
    session.execute.return_value.scalars.return_value.all.return_value = list(values)
    return session


@pytest.fixture(autouse=True)
def active_version():
    with patch("app.repositories.levy.get_active_version", return_value=3) as gav:
        yield gav


def test_parses_numeric_text_to_int():
    assert resolve_edp_id(_session_returning_ids("1"), LABEL) == 1


def test_no_matching_row_means_no_id():
    assert resolve_edp_id(_session_returning_ids(), LABEL) is None


@pytest.mark.parametrize("raw", [None, "", "  ", "abc", "1.5"])
def test_non_integer_values_mean_no_id(raw):
    assert resolve_edp_id(_session_returning_ids(raw), LABEL) is None


def test_a_label_with_two_ids_means_no_id():
    assert resolve_edp_id(_session_returning_ids("1", "2"), LABEL) is None


def test_queries_the_active_boundary_layer_version(active_version):
    session = _session_returning_ids("1")

    resolve_edp_id(session, LABEL)

    active_version.assert_called_once_with(session, "edp_boundary_layer")


# --- get_inflation_index -----------------------------------------------------


def test_get_inflation_index_returns_the_row():
    index_row = MagicMock()
    session = _session_returning(index_row)

    assert get_inflation_index(session, 2026) is index_row


def test_get_inflation_index_returns_none_when_missing():
    session = _session_returning(None)

    assert get_inflation_index(session, 2099) is None


# --- record_levy_calculation -------------------------------------------------


def test_record_maps_every_audit_field_and_adds_to_session():
    session = MagicMock()

    row = record_levy_calculation(session, "NRL-000001", make_levy_calculation())

    session.add.assert_called_once_with(row)
    assert isinstance(row, LevyCalculationRecord)
    assert row.quote_reference == "NRL-000001"
    assert row.edp_id == 1
    assert row.edp_name == EDP_NAME
    assert row.edp_start_date == date(2026, 1, 1)
    assert row.calculator_version == 1
    assert row.base_charge_per_unit == Decimal("2193.6649")
    assert row.units == 10
    assert row.calculation_date == date(2026, 9, 14)
    assert row.provisional_amount == Decimal("21936.60")
    assert row.inflation_adjusted_amount == Decimal("21936.60")
    assert row.levy_charge_id == LEVY_CHARGE_ID
    assert row.edp_start_year_index_id is None
    assert row.edp_start_year_index_factor is None
    assert row.calculation_year_index_id is None
    assert row.calculation_year_index_factor is None


def test_record_maps_the_applied_index_rows():
    start_id, calc_id = uuid4(), uuid4()
    levy = make_levy_calculation(
        edp_start_year_index_id=start_id,
        edp_start_year_index_factor=Decimal("1.0000"),
        calculation_year_index_id=calc_id,
        calculation_year_index_factor=Decimal("1.0512"),
    )

    row = record_levy_calculation(MagicMock(), "NRL-000001", levy)

    assert row.edp_start_year_index_id == start_id
    assert row.edp_start_year_index_factor == Decimal("1.0000")
    assert row.calculation_year_index_id == calc_id
    assert row.calculation_year_index_factor == Decimal("1.0512")
