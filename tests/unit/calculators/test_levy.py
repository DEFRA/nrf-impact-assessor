"""Provisional = units x base price. Each year from the base price's effective
year to the calculation year is provisional x that year's index / the base
year's index, rounded half-up to pence. The last line is the adjusted amount.
Under £50 is £0 with no lines."""

from datetime import UTC, date, datetime
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import pytest

from app.calculators.levy import (
    MAX_LEVY_UNITS,
    LevyAmountTooLargeError,
    LevyCalculation,
    LevyChargeUnavailableError,
    LevyUnitsTooLargeError,
    calculate_levy,
    round_gbp,
)

INDEX = {2026: "400", 2027: "412", 2028: "417", 2029: "422", 2030: "427"}


def _charge(
    price: str, effective_year: int = 2027, original_price: str | None = None
) -> SimpleNamespace:
    return SimpleNamespace(
        edp_id=1,
        edp_start_date=date(2026, 1, 1),
        base_charge_per_unit=Decimal(price),
        effective_from=datetime(effective_year, 1, 1, tzinfo=UTC),
        original_base_charge_per_unit=Decimal(original_price or price),
    )


def _index(year: int, value: str) -> SimpleNamespace:
    return SimpleNamespace(id=uuid4(), charging_year=year, cil_index=Decimal(value))


def _indices(first: int, last: int, **values: str) -> list[SimpleNamespace]:
    return [
        _index(y, values.get(f"y{y}", INDEX.get(y, "400")))
        for y in range(first, last + 1)
    ]


def _levy(price: str, units: int, year: int, base_year: int = 2027, **values: str):
    return calculate_levy(
        _charge(price, base_year),
        units,
        date(year, 6, 24),
        _indices(base_year, year, **values),
    )


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("1234.5649", "1234.56"),
        ("1234.5650", "1234.57"),
        ("1234.565", "1234.57"),  # banker's rounding would give .56
        ("1234.56", "1234.56"),
        ("0.005", "0.01"),
    ],
)
def test_round_gbp_is_half_up_to_two_places(raw, expected):
    assert round_gbp(Decimal(raw)) == Decimal(expected)


def test_round_gbp_keeps_two_place_scale():
    assert str(round_gbp(Decimal("10"))) == "10.00"


def test_base_year_amount_is_the_provisional_amount():
    levy = _levy("2675.00", 1000, 2027)

    assert levy.inflation_adjusted_amount == levy.provisional_amount


def test_one_line_per_year_from_base_year_and_last_line_is_the_amount():
    levy = _levy("2675.00", 1000, 2030)

    assert [(line.year, line.amount) for line in levy.yearly_lines] == [
        (2027, Decimal("2675000.00")),
        (2028, Decimal("2707463.59")),
        (2029, Decimal("2739927.18")),
        (2030, Decimal("2772390.78")),
    ]
    assert levy.yearly_lines[0].index_value == Decimal("412")
    assert levy.yearly_lines[-1].index_value == Decimal("427")


def test_lines_are_not_compounded():
    # 100 x 302/300 = 100.67. Compounding from the rounded 2028 line would give
    # 100.33 x 302/301 = 100.66.
    levy = _levy("100.00", 1, 2029, y2027="300", y2028="301", y2029="302")

    assert [line.amount for line in levy.yearly_lines] == [
        Decimal("100.00"),
        Decimal("100.33"),
        Decimal("100.67"),
    ]


def test_example_b_new_base_price_resets_the_base_year():
    levy = _levy("3000.00", 1000, 2030, base_year=2029)

    assert [line.year for line in levy.yearly_lines] == [2029, 2030]
    assert levy.inflation_adjusted_amount == Decimal("3035545.02")


def test_lines_name_the_index_rows():
    indices = _indices(2027, 2028)
    levy = calculate_levy(_charge("2675.00"), 1, date(2028, 1, 1), indices)

    assert [line.index_id for line in levy.yearly_lines] == [i.id for i in indices]
    assert levy.base_price_effective_from == datetime(2027, 1, 1, tzinfo=UTC)


def test_indices_outside_the_range_are_ignored():
    indices = _indices(2026, 2029)
    levy = calculate_levy(_charge("2675.00"), 1, date(2028, 1, 1), indices)

    assert [line.year for line in levy.yearly_lines] == [2027, 2028]


def test_under_50_pounds_is_zero_with_no_lines():
    levy = calculate_levy(_charge("49.99"), 1, date(2028, 6, 24), [])

    assert levy.provisional_amount == Decimal("49.99")
    assert levy.inflation_adjusted_amount == Decimal("0.00")
    assert levy.yearly_lines == []
    assert levy.charged_amount == Decimal("0.00")


def test_exactly_50_pounds_is_inflated():
    levy = _levy("50.00", 1, 2028)

    assert levy.inflation_adjusted_amount == Decimal("50.61")
    assert levy.charged_amount == Decimal("50.00")


def test_missing_middle_year_index_is_unavailable():
    indices = [_index(2027, "412"), _index(2029, "422")]

    with pytest.raises(LevyChargeUnavailableError, match="2028"):
        calculate_levy(_charge("2675.00"), 1, date(2029, 6, 24), indices)


def test_missing_base_year_index_is_unavailable():
    with pytest.raises(LevyChargeUnavailableError, match="2027"):
        calculate_levy(_charge("2675.00"), 1, date(2028, 6, 24), [_index(2028, "417")])


def test_calculation_before_the_base_year_is_unavailable():
    with pytest.raises(LevyChargeUnavailableError):
        calculate_levy(_charge("2675.00"), 1, date(2026, 6, 24), _indices(2026, 2027))


@pytest.mark.parametrize(
    ("price", "expected"),
    [("2675", "8025.00"), ("2675.5", "8026.50"), ("2675.01", "8025.03")],
)
def test_provisional_is_base_charge_times_units(price, expected):
    levy = _levy(price, 3, 2027)

    assert str(levy.provisional_amount) == expected


def test_rounds_the_total_not_the_per_unit_charge():
    # 1000 x 2 / 3 = 666.666...; rounding per unit first would give 666.66.
    levy = _levy("1000", 2, 2028, y2027="3", y2028="1")

    assert levy.inflation_adjusted_amount == Decimal("666.67")


def test_inflated_total_rounds_half_up():
    # 2 x 17 / 16 = 2.125 rounds up (banker's gives 2.12); exactly £50+ to inflate.
    levy = _levy("1700", 2, 2028, y2027="1600", y2028="1")

    assert levy.inflation_adjusted_amount == Decimal("2.13")


def test_prices_a_levy_up_to_the_largest_amount():
    levy = _levy("99999999.99", 100, 2027)

    assert levy.provisional_amount == Decimal("9999999999.00")


def test_rejects_a_provisional_levy_too_large():
    with pytest.raises(LevyAmountTooLargeError, match="exceeds") as exc:
        _levy("99999999.99", 101, 2027)

    assert exc.value.amount == Decimal("10099999998.99")
    assert exc.value.units == 101
    assert isinstance(exc.value, LevyChargeUnavailableError)


def test_rejects_an_original_base_amount_too_large():
    charge = _charge("2675.00", original_price="99999999.99")

    with pytest.raises(LevyAmountTooLargeError, match="exceeds") as exc:
        calculate_levy(charge, 101, date(2027, 6, 24), _indices(2027, 2027))

    assert exc.value.amount == Decimal("10099999998.99")


def test_rejects_an_inflated_line_too_large():
    with pytest.raises(LevyAmountTooLargeError, match="exceeds"):
        _levy("99999999.99", 100, 2028, y2027="10000000", y2028="10000001")


def test_an_intermediate_line_over_the_largest_amount_is_not_rejected():
    # Only the provisional and final amounts are sent to nrf-backend.
    levy = _levy("99999999.99", 100, 2029, y2027="400", y2028="800", y2029="400")

    assert levy.yearly_lines[1].amount == Decimal("19999999998.00")
    assert levy.inflation_adjusted_amount == Decimal("9999999999.00")


@pytest.mark.parametrize(
    ("calculation_index", "fits"),
    [("9999999999994", True), ("9999999999995", False)],
)
def test_inflated_levy_that_rounds_to_the_largest_amount_fits(calculation_index, fits):
    def calculate():
        return _levy(
            "99999999.99", 100, 2028, y2027="9999999999000", y2028=calculation_index
        )

    if fits:
        assert calculate().inflation_adjusted_amount == Decimal("9999999999.99")
    else:
        with pytest.raises(LevyAmountTooLargeError):
            calculate()


def test_prices_up_to_the_most_units():
    assert _levy("0.01", MAX_LEVY_UNITS, 2027).units == 2_147_483_647


def test_rejects_more_units_than_the_most():
    with pytest.raises(LevyUnitsTooLargeError, match="exceeds") as exc:
        _levy("0.01", MAX_LEVY_UNITS + 1, 2027)

    assert exc.value.units == 2_147_483_648
    assert isinstance(exc.value, LevyChargeUnavailableError)


def test_carries_the_charge_fields():
    levy = _levy("2675.00", 10, 2027)

    assert isinstance(levy, LevyCalculation)
    assert levy.edp_id == 1
    assert levy.edp_start_date == date(2026, 1, 1)
    assert levy.units == 10


def test_original_base_is_the_original_charge_times_units_uninflated():
    result = calculate_levy(
        _charge("2675.00", 2026, original_price="2500.50"),
        10,
        date(2028, 6, 24),
        _indices(2026, 2028),
    )

    assert result.original_base_charge_per_unit == Decimal("2500.50")
    assert result.original_base_amount == Decimal("25005.00")
    assert str(result.original_base_amount) == "25005.00"


@pytest.mark.parametrize("units", [0, -1])
def test_rejects_non_positive_units(units):
    with pytest.raises(ValueError, match="units"):
        _levy("2675.00", units, 2027)
