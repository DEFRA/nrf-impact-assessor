"""The levy calculation as nrf-backend and nrf-frontend receive it.

One model serves both the recalculate endpoint response and the quote PATCH
payload, so the audit fields the frontend relays at check your answers and the
ones sent when a quote is created cannot drift apart.
"""

from datetime import datetime

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

from app.calculators.levy import LevyCalculation


class LevyWire(BaseModel):
    """A levy calculation with every audit field, camelCase on the wire."""

    model_config = ConfigDict(
        frozen=True, alias_generator=to_camel, populate_by_name=True
    )

    calculation_date: datetime
    provisional_amount: float
    inflation_adjusted_amount: float
    original_base_amount: float
    base_charge_per_unit: float
    original_base_charge_per_unit: float
    calculator_version: int
    units: int

    @classmethod
    def from_calculation(
        cls, levy: LevyCalculation, calculated_at: datetime
    ) -> LevyWire:
        """calculated_at is the moment of calculation; levy.calculation_date
        is only the date the price was looked up for.

        The original base amount and charge per unit are stubs until the
        calculator records them: they repeat the base price in force and the
        provisional amount.
        """
        return cls(
            calculation_date=calculated_at,
            provisional_amount=float(levy.provisional_amount),
            inflation_adjusted_amount=float(levy.inflation_adjusted_amount),
            original_base_amount=float(levy.provisional_amount),
            base_charge_per_unit=float(levy.base_charge_per_unit),
            original_base_charge_per_unit=float(levy.base_charge_per_unit),
            calculator_version=levy.calculator_version,
            units=levy.units,
        )
