"""resolve_edp_id parses attributes->>'EDP_Id' strictly: anything that is not
an integer is 'no id', so the caller fails closed (spec decision 6).
record_levy_calculation maps every scenario 7 field onto the audit row."""

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from app.calculators.levy import LevyCalculation
from app.models.db import LevyCalculationRecord
from app.repositories.levy import (
    get_inflation_index,
    record_levy_calculation,
    resolve_edp_id,
)

LABEL = "Broads SAC EDP"


def _session_returning(value) -> MagicMock:
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = value
    return session


@pytest.fixture(autouse=True)
def active_version():
    with patch("app.repositories.levy.get_active_version", return_value=3) as gav:
        yield gav


def test_parses_numeric_text_to_int():
    assert resolve_edp_id(_session_returning("1"), LABEL) == 1


@pytest.mark.parametrize("raw", [None, "", "  ", "abc", "1.5"])
def test_non_integer_values_mean_no_id(raw):
    assert resolve_edp_id(_session_returning(raw), LABEL) is None


def test_queries_the_active_boundary_layer_version(active_version):
    session = _session_returning("1")

    resolve_edp_id(session, LABEL)

    active_version.assert_called_once_with(session, "edp_boundary_layer")


# --- get_inflation_index -----------------------------------------------------


def test_get_inflation_index_returns_value():
    session = _session_returning(Decimal("400"))

    assert get_inflation_index(session, 2026) == Decimal("400")


def test_get_inflation_index_returns_none_when_missing():
    session = _session_returning(None)

    assert get_inflation_index(session, 2099) is None


# --- record_levy_calculation -------------------------------------------------


def _levy() -> LevyCalculation:
    return LevyCalculation(
        edp_id=1,
        edp_name="Norfolk EDP",
        edp_start_date=date(2026, 1, 1),
        calculation_date=date(2026, 9, 14),
        calculator_version=1,
        units=10,
        base_charge_per_unit=Decimal("2193.6649"),
        rounded_charge_per_unit=Decimal("2193.66"),
        provisional_amount=Decimal("21936.60"),
        inflation_adjusted_amount=Decimal("21936.60"),
    )


def test_record_maps_every_audit_field_and_adds_to_session():
    session = MagicMock()

    row = record_levy_calculation(session, "NRL-000001", _levy())

    session.add.assert_called_once_with(row)
    assert isinstance(row, LevyCalculationRecord)
    assert row.quote_reference == "NRL-000001"
    assert row.edp_id == 1
    assert row.edp_name == "Norfolk EDP"
    assert row.edp_start_date == date(2026, 1, 1)
    assert row.calculator_version == 1
    assert row.base_charge_per_unit == Decimal("2193.6649")
    assert row.rounded_charge_per_unit == Decimal("2193.66")
    assert row.units == 10
    assert row.calculation_date == date(2026, 9, 14)
    assert row.provisional_amount == Decimal("21936.60")
    assert row.inflation_adjusted_amount == Decimal("21936.60")
