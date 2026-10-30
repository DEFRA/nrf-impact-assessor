"""A base charge of at most 2 dp times units, and half-up rounding to 2 dp of
the inflation-adjusted charge only."""

from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import pytest

from app.calculators.levy import (
    LEVY_CALCULATOR_VERSION,
    MAX_LEVY_UNITS,
    LevyAmountTooLargeError,
    LevyCalculation,
    LevyChargeUnavailableError,
    LevyUnitsTooLargeError,
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


def test_provisional_is_base_charge_times_units():
    # The NE-approved base charge, 10 units.
    result = calculate_levy(_charge("2675.0000"), units=10, calculation_date=CALC_DATE)

    assert result.base_charge_per_unit == Decimal("2675.0000")
    assert result.provisional_amount == Decimal("26750.00")
    assert str(result.provisional_amount) == "26750.00"


@pytest.mark.parametrize("price", ["2675", "2675.00"])
def test_accepts_whole_base_charge_at_any_scale(price):
    result = calculate_levy(_charge(price), units=3, calculation_date=CALC_DATE)

    assert result.provisional_amount == Decimal("8025.00")


@pytest.mark.parametrize(
    ("price", "expected"),
    [
        ("2675.50", "8026.50"),
        ("2675.5", "8026.50"),
        ("2675.5000", "8026.50"),
        ("2675.01", "8025.03"),
        ("0.99", "2.97"),
    ],
)
def test_prices_base_charge_with_pence(price, expected):
    result = calculate_levy(_charge(price), units=3, calculation_date=CALC_DATE)

    assert result.provisional_amount == Decimal(expected)
    assert str(result.provisional_amount) == expected


def test_prices_a_levy_up_to_the_largest_audit_amount():
    result = calculate_levy(
        _charge("99999999.99"), units=100, calculation_date=CALC_DATE
    )

    assert result.provisional_amount == Decimal("9999999999.00")


def test_rejects_a_provisional_levy_too_large_for_the_audit_record():
    with pytest.raises(LevyAmountTooLargeError, match="exceeds") as exc:
        calculate_levy(_charge("99999999.99"), units=101, calculation_date=CALC_DATE)

    assert exc.value.amount == Decimal("10099999998.99")
    assert exc.value.units == 101
    # Still a LevyChargeUnavailableError, so the quote job stops as before.
    assert isinstance(exc.value, LevyChargeUnavailableError)


def test_rejects_an_inflated_levy_too_large_for_the_audit_record():
    # Provisional 9,999,999,999.00 fits; inflated by 1.0000001 it does not.
    with pytest.raises(LevyAmountTooLargeError, match="exceeds"):
        calculate_levy(
            _charge("99999999.99"),
            units=100,
            calculation_date=EARLIER_CALC_DATE,
            edp_start_year_index=_index("1"),
            calculation_year_index=_index("1.0000001"),
        )


def test_prices_up_to_the_most_units_the_audit_record_holds():
    result = calculate_levy(
        _charge("0.01"), units=MAX_LEVY_UNITS, calculation_date=CALC_DATE
    )

    assert result.units == 2_147_483_647


def test_rejects_more_units_than_the_audit_record_holds():
    # £0.01 x 2**31 units is well under MAX_LEVY_AMOUNT, but
    # audit_levy_calculations.units is a PostgreSQL integer.
    with pytest.raises(LevyUnitsTooLargeError, match="exceeds") as exc:
        calculate_levy(
            _charge("0.01"), units=MAX_LEVY_UNITS + 1, calculation_date=CALC_DATE
        )

    assert exc.value.units == 2_147_483_648
    assert isinstance(exc.value, LevyChargeUnavailableError)


def test_rejects_a_levy_with_more_digits_than_decimal_precision():
    # Rounding 10**24 x £2,675.12 to pence needs more than 28 digits.
    with pytest.raises(LevyAmountTooLargeError, match="exceeds"):
        calculate_levy(_charge("2675.12"), units=10**24, calculation_date=CALC_DATE)


def test_rejects_an_inflated_levy_with_more_digits_than_decimal_precision():
    with pytest.raises(LevyAmountTooLargeError, match="exceeds"):
        calculate_levy(
            _charge("2675.12"),
            units=10**24,
            calculation_date=EARLIER_CALC_DATE,
            edp_start_year_index=_index("3"),
            calculation_year_index=_index("7"),
        )


@pytest.mark.parametrize(
    ("calculation_year_factor", "fits"),
    [
        # 9,999,999,999.00 inflated to exactly 9,999,999,999.994 rounds down to
        # the largest audit amount; to 9,999,999,999.995 it rounds past it.
        ("9999999999994", True),
        ("9999999999995", False),
    ],
)
def test_inflated_levy_that_rounds_to_the_largest_audit_amount_fits(
    calculation_year_factor, fits
):
    def calculate():
        return calculate_levy(
            _charge("99999999.99"),
            units=100,
            calculation_date=EARLIER_CALC_DATE,
            edp_start_year_index=_index("9999999999000"),
            calculation_year_index=_index(calculation_year_factor),
        )

    if fits:
        assert calculate().inflation_adjusted_amount == Decimal("9999999999.99")
    else:
        with pytest.raises(LevyAmountTooLargeError):
            calculate()


def test_provisional_rounds_half_up_like_the_inflated_total():
    # The levy_charges CHECK keeps such a charge out of the database; a caller
    # that bypasses it still gets the module's half-up rule, not banker's.
    result = calculate_levy(
        _charge("2675.125"),
        units=1,
        calculation_date=EARLIER_CALC_DATE,
        edp_start_year_index=_index("1"),
        calculation_year_index=_index("1"),
    )

    assert result.provisional_amount == Decimal("2675.13")
    assert result.inflation_adjusted_amount == Decimal("2675.13")


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


def test_skips_inflation_when_not_applied_even_if_indices_are_missing():
    result = calculate_levy(
        _charge("2675.0000"),
        units=10,
        calculation_date=EARLIER_CALC_DATE,
        apply_inflation=False,
    )

    assert result.provisional_amount == Decimal("26750.00")
    assert result.inflation_adjusted_amount == result.provisional_amount
    assert result.edp_start_year_index_id is None
    assert result.calculation_year_index_id is None


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
