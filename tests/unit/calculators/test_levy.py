"""Scenarios 1 and 3 of NRF2-913: a whole-pound base charge times units, and
half-up rounding to 2 dp of the inflation-adjusted charge only."""

from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import pytest

from app.calculators.levy import (
    LEVY_CALCULATOR_VERSION,
    LevyCalculation,
    LevyChargeUnavailableError,
    calculate_levy,
    round_gbp,
)

CALC_DATE = date(2026, 9, 14)
EARLIER_CALC_DATE = date(2022, 6, 1)  # an earlier charging year than the EDP start
CHARGE_ID = uuid4()


def _charge(price: str) -> SimpleNamespace:
    return SimpleNamespace(
        id=CHARGE_ID,
        edp_id=1,
        edp_name="Norfolk EDP",
        edp_start_date=date(2026, 1, 1),
        base_charge_per_unit=Decimal(price),
    )


def _index(factor: str) -> SimpleNamespace:
    return SimpleNamespace(id=uuid4(), index_factor=Decimal(factor))


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("1234.5649", "1234.56"),  # third decimal 4: down
        ("1234.5650", "1234.57"),  # third decimal 5: up
        ("1234.565", "1234.57"),  # banker's rounding would give .56
        ("1234.56", "1234.56"),  # already 2 dp
        ("0.005", "0.01"),
    ],
)
def test_round_gbp_is_half_up_to_two_places(raw, expected):
    assert round_gbp(Decimal(raw)) == Decimal(expected)


def test_round_gbp_keeps_two_place_scale():
    assert str(round_gbp(Decimal("10"))) == "10.00"


def test_provisional_is_whole_base_charge_times_units():
    # NRF2-1242: the NE-approved base charge, 10 units.
    result = calculate_levy(_charge("2675.0000"), units=10, calculation_date=CALC_DATE)

    assert result.base_charge_per_unit == Decimal("2675.0000")
    assert result.provisional_amount == Decimal("26750.00")
    assert str(result.provisional_amount) == "26750.00"


@pytest.mark.parametrize("price", ["2675", "2675.00"])
def test_accepts_whole_base_charge_at_any_scale(price):
    result = calculate_levy(_charge(price), units=3, calculation_date=CALC_DATE)

    assert result.provisional_amount == Decimal("8025.00")


@pytest.mark.parametrize("price", ["2193.6649", "2675.01", "0.5"])
def test_rejects_fractional_base_charge(price):
    with pytest.raises(LevyChargeUnavailableError, match="whole number"):
        calculate_levy(_charge(price), units=10, calculation_date=CALC_DATE)


@pytest.mark.parametrize(
    ("price", "start_factor", "expected"),
    [
        ("1000", "6", "333.33"),  # 2 x 1000 / 6 = 333.333...: third decimal 3, down
        ("17", "16", "2.13"),  # 2 x 17 / 16 = 2.125: 5, up (banker's gives 2.12)
    ],
)
def test_inflation_adjusted_total_is_rounded_half_up(price, start_factor, expected):
    result = calculate_levy(
        _charge(price),
        units=2,
        calculation_date=EARLIER_CALC_DATE,
        edp_start_year_index=_index(start_factor),
        calculation_year_index=_index("1"),
    )

    assert result.inflation_adjusted_amount == Decimal(expected)


def test_rounds_the_inflated_total_not_the_per_unit_charge():
    # 1000 / 3 = 333.333... per unit. Rounding per unit first would give
    # 333.33 x 2 = 666.66; the rule rounds the total, 666.666... -> 666.67.
    result = calculate_levy(
        _charge("1000"),
        units=2,
        calculation_date=EARLIER_CALC_DATE,
        edp_start_year_index=_index("3"),
        calculation_year_index=_index("1"),
    )

    assert result.inflation_adjusted_amount == Decimal("666.67")


@pytest.mark.parametrize(
    ("edp_start_year_index", "calculation_year_index"),
    [(None, None), (_index("400"), _index("999"))],
    ids=["no_indices", "indices_present_but_ignored"],
)
def test_no_adjustment_when_calculation_year_equals_edp_start_year(
    edp_start_year_index, calculation_year_index
):
    charge = _charge("2675.0000")  # edp_start_date = 2026-01-01
    result = calculate_levy(
        charge,
        units=10,
        calculation_date=date(2026, 12, 31),
        edp_start_year_index=edp_start_year_index,
        calculation_year_index=calculation_year_index,
    )

    assert result.inflation_adjusted_amount == result.provisional_amount
    # Indices passed but not applied are not recorded as used.
    assert result.edp_start_year_index_id is None
    assert result.edp_start_year_index_factor is None
    assert result.calculation_year_index_id is None
    assert result.calculation_year_index_factor is None


def test_applies_index_ratio_when_calculation_year_differs():
    charge = _charge("2675.0000")  # edp_start_date = 2026-01-01
    result = calculate_levy(
        charge,
        units=10,
        calculation_date=EARLIER_CALC_DATE,
        edp_start_year_index=_index("1.0000"),  # 2026 factor
        calculation_year_index=_index("0.8300"),  # 2022 factor
    )

    # round_gbp(2675 * 10 * 0.83) = 22202.50
    assert result.inflation_adjusted_amount == Decimal("22202.50")
    assert result.provisional_amount == Decimal("26750.00")


def test_records_the_index_rows_it_applied():
    start, calc = _index("1.0000"), _index("0.8300")
    result = calculate_levy(
        _charge("2675.0000"),
        units=10,
        calculation_date=EARLIER_CALC_DATE,
        edp_start_year_index=start,
        calculation_year_index=calc,
    )

    assert result.edp_start_year_index_id == start.id
    assert result.edp_start_year_index_factor == Decimal("1.0000")
    assert result.calculation_year_index_id == calc.id
    assert result.calculation_year_index_factor == Decimal("0.8300")


def test_raises_when_calculation_year_differs_and_index_missing():
    charge = _charge("2675.0000")

    with pytest.raises(LevyChargeUnavailableError, match="inflation index"):
        calculate_levy(charge, units=10, calculation_date=EARLIER_CALC_DATE)


def test_carries_audit_fields():
    result = calculate_levy(_charge("2675.0000"), units=10, calculation_date=CALC_DATE)

    assert isinstance(result, LevyCalculation)
    assert result.levy_charge_id == CHARGE_ID
    assert result.edp_id == 1
    assert result.edp_name == "Norfolk EDP"
    assert result.edp_start_date == date(2026, 1, 1)
    assert result.calculation_date == CALC_DATE
    assert result.units == 10
    assert result.calculator_version == LEVY_CALCULATOR_VERSION == 1


@pytest.mark.parametrize("units", [0, -1])
def test_rejects_non_positive_units(units):
    charge = _charge("2675.0000")

    with pytest.raises(ValueError, match="units"):
        calculate_levy(charge, units=units, calculation_date=CALC_DATE)
