"""Tests for the levy calculation wire model."""

from decimal import Decimal

from app.levy.wire import LevyWire
from tests.conftest import CALCULATED_AT, make_levy_calculation


def test_original_base_values_repeat_the_base_price_and_provisional_amount():
    levy = make_levy_calculation(
        base_charge_per_unit=Decimal("2675.00"),
        provisional_amount=Decimal("26750.00"),
    )

    wire = LevyWire.from_calculation(levy, CALCULATED_AT)

    assert wire.original_base_amount == 26750.0
    assert wire.original_base_charge_per_unit == 2675.0


def test_calculation_date_is_the_moment_of_calculation_not_the_priced_date():
    levy = make_levy_calculation()

    wire = LevyWire.from_calculation(levy, CALCULATED_AT)

    assert wire.calculation_date == CALCULATED_AT
