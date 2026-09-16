"""Scenario 1 and 3 of NRF2-913: half-up rounding to 2 dp, then units x rounded."""

from datetime import date
from decimal import Decimal
from types import SimpleNamespace

import pytest

from app.calculators.levy import (
    LEVY_CALCULATOR_VERSION,
    LevyCalculation,
    LevyChargeUnavailableError,
    calculate_levy,
    round_gbp,
)

CALC_DATE = date(2026, 9, 14)


def _charge(price: str) -> SimpleNamespace:
    return SimpleNamespace(
        edp_id=1,
        edp_name="Norfolk EDP",
        edp_start_date=date(2026, 1, 1),
        base_charge_per_unit=Decimal(price),
    )


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("2193.6649", "2193.66"),  # third decimal 4: down
        ("2193.6650", "2193.67"),  # third decimal 5: up
        ("2193.665", "2193.67"),  # banker's rounding would give .66
        ("2193.66", "2193.66"),  # already 2 dp
        ("0.005", "0.01"),
    ],
)
def test_round_gbp_is_half_up_to_two_places(raw, expected):
    assert round_gbp(Decimal(raw)) == Decimal(expected)


def test_round_gbp_keeps_two_place_scale():
    assert str(round_gbp(Decimal("10"))) == "10.00"


def test_worked_example_from_ticket():
    result = calculate_levy(_charge("2193.6649"), units=10, calculation_date=CALC_DATE)

    assert result.base_charge_per_unit == Decimal("2193.6649")
    assert result.rounded_charge_per_unit == Decimal("2193.66")
    assert result.provisional_amount == Decimal("21936.60")
    assert str(result.provisional_amount) == "21936.60"


def test_rounds_the_charge_before_multiplying_not_after():
    # 3 x 2193.665 = 6580.995 -> 6581.00 if rounded after; the rule rounds first.
    result = calculate_levy(_charge("2193.665"), units=3, calculation_date=CALC_DATE)

    assert result.rounded_charge_per_unit == Decimal("2193.67")
    assert result.provisional_amount == Decimal("6581.01")


@pytest.mark.parametrize(
    ("edp_start_year_index", "calculation_year_index"),
    [(None, None), (Decimal("400"), Decimal("999"))],
    ids=["no_indices", "indices_present_but_ignored"],
)
def test_no_adjustment_when_calculation_year_equals_edp_start_year(
    edp_start_year_index, calculation_year_index
):
    charge = _charge("2193.6649")  # edp_start_date = 2026-01-01
    result = calculate_levy(
        charge,
        units=10,
        calculation_date=date(2026, 12, 31),
        edp_start_year_index=edp_start_year_index,
        calculation_year_index=calculation_year_index,
    )

    assert result.inflation_adjusted_amount == result.provisional_amount


def test_applies_index_ratio_when_calculation_year_differs():
    charge = _charge("2193.6649")  # edp_start_date = 2026-01-01
    result = calculate_levy(
        charge,
        units=10,
        calculation_date=date(2022, 6, 1),
        edp_start_year_index=Decimal("1.0000"),  # 2026 factor
        calculation_year_index=Decimal("0.8300"),  # 2022 factor
    )

    # round_gbp(2193.6649 * 0.83) * 10 = 1820.74 * 10
    assert result.inflation_adjusted_amount == Decimal("18207.40")
    assert result.provisional_amount == Decimal("21936.60")


def test_raises_when_calculation_year_differs_and_index_missing():
    charge = _charge("2193.6649")

    with pytest.raises(LevyChargeUnavailableError, match="inflation index"):
        calculate_levy(charge, units=10, calculation_date=date(2022, 6, 1))


def test_carries_audit_fields():
    result = calculate_levy(_charge("2193.6649"), units=10, calculation_date=CALC_DATE)

    assert isinstance(result, LevyCalculation)
    assert result.edp_id == 1
    assert result.edp_name == "Norfolk EDP"
    assert result.edp_start_date == date(2026, 1, 1)
    assert result.calculation_date == CALC_DATE
    assert result.units == 10
    assert result.calculator_version == LEVY_CALCULATOR_VERSION == 1


@pytest.mark.parametrize("units", [0, -1])
def test_rejects_non_positive_units(units):
    with pytest.raises(ValueError, match="units"):
        calculate_levy(_charge("2193.6649"), units=units, calculation_date=CALC_DATE)
