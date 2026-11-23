"""Nature restoration levy calculation.

Provisional = units x the base price in force. Under £50 the adjusted amount
is £0. Otherwise there is one line per charging year from the base price's
effective year to the calculation year: provisional x that year's RICS CIL
index / the effective year's index, rounded half-up to pence, never compounded.
The last line is the inflation-adjusted amount. Charging years are UTC
calendar years. Python's round() is banker's rounding, so it is never used.
"""

from collections.abc import Sequence
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Protocol
from uuid import UUID

from pydantic import BaseModel, Field

# Sent to nrf-backend as modelVersion.
LEVY_CALCULATOR_VERSION = 1
MIN_LEVY_AMOUNT = Decimal("50")

_PENNY = Decimal("0.01")
_ZERO_GBP = Decimal("0.00")
# The largest amount nrf-backend's quote_edp_results numeric(12,2) levy columns hold.
MAX_LEVY_AMOUNT = Decimal("9999999999.99")
# The smallest unrounded amount that rounds half-up past MAX_LEVY_AMOUNT.
_FIRST_AMOUNT_TOO_LARGE = MAX_LEVY_AMOUNT + _PENNY / 2
# The most units nrf-backend's quote_edp_results integer units column holds. A cheap
# enough charge prices more units than this within MAX_LEVY_AMOUNT.
MAX_LEVY_UNITS = 2**31 - 1


class LevyChargeUnavailableError(RuntimeError):
    """The data needed to calculate the levy for a job is missing.

    Raised out of process_job so the consumer leaves the SQS message on the
    queue (it ages to the DLQ), no PATCH callback is sent, and therefore no
    quote email goes out with an assumed or default value.
    """


class LevyAmountTooLargeError(LevyChargeUnavailableError):
    """The levy exceeds MAX_LEVY_AMOUNT, so it cannot be stored in the backend.

    Carries the amount and units so the recalculate endpoint can tell the
    caller why, unlike the other reasons a levy is unavailable.
    """

    def __init__(self, amount: Decimal, units: int, edp_id: int) -> None:
        self.amount = amount
        self.units = units
        super().__init__(
            f"levy amount {amount} for edp_id={edp_id}, units={units} "
            f"exceeds the {MAX_LEVY_AMOUNT} a quote_edp_results row holds"
        )


class LevyUnitsTooLargeError(LevyChargeUnavailableError):
    """units exceeds MAX_LEVY_UNITS, so it cannot be stored in the backend.

    Carries the units so the recalculate endpoint can tell the caller why.
    """

    def __init__(self, units: int, edp_id: int) -> None:
        self.units = units
        super().__init__(
            f"units={units} for edp_id={edp_id} exceeds the {MAX_LEVY_UNITS} "
            "a quote_edp_results row holds"
        )


class LevyChargeRow(Protocol):
    """The charge fields the calculation reads: the base price in force for an
    EDP, when it took effect, and the EDP's original (oldest) base price.

    Structural rather than an ORM model, so this module stays free of database
    imports and a test can pass any object with these attributes.
    """

    edp_id: int
    edp_start_date: date
    base_price_per_unit: Decimal
    base_price_effective_from: datetime
    original_base_price_per_unit: Decimal


class LevyIndexRow(Protocol):
    id: UUID
    charging_year: int
    cil_index: Decimal


class LevyYearLine(BaseModel):
    year: int
    index_id: UUID
    index_value: Decimal
    amount: Decimal


class LevyCalculation(BaseModel):
    """One levy calculation, carrying the fields sent to nrf-backend.

    original_base_price_per_unit is the EDP's oldest base price and
    original_base_amount is the levy for units at that price, uninflated.
    yearly_lines is empty only under MIN_LEVY_AMOUNT.
    """

    edp_id: int
    edp_start_date: date
    calculator_version: int
    units: int = Field(gt=0)
    base_price_per_unit: Decimal
    base_price_effective_from: datetime
    original_base_price_per_unit: Decimal
    provisional_amount: Decimal
    original_base_amount: Decimal
    inflation_adjusted_amount: Decimal
    yearly_lines: list[LevyYearLine] = []

    @property
    def charged_amount(self) -> Decimal:
        """The levy charged: the provisional amount, or £0 under MIN_LEVY_AMOUNT."""
        if self.provisional_amount < MIN_LEVY_AMOUNT:
            return _ZERO_GBP
        return self.provisional_amount


def round_gbp(value: Decimal) -> Decimal:
    """Round to 2 decimal places, half-up, keeping a 2-place scale."""
    return value.quantize(_PENNY, rounding=ROUND_HALF_UP)


def needs_inflation(charge: LevyChargeRow, units: int) -> bool:
    """Whether the levy is inflated, so calculate_levy needs its index rows:
    a provisional amount under MIN_LEVY_AMOUNT is charged as £0."""
    return charge.base_price_per_unit * units >= MIN_LEVY_AMOUNT


def calculate_levy(
    charge: LevyChargeRow,
    units: int,
    calculation_date: date,
    indices: Sequence[LevyIndexRow],
) -> LevyCalculation:
    """Calculate the provisional and inflation-adjusted levy for one EDP.

    `indices` must hold a row for every year from charge.base_price_effective_from's year
    to calculation_date's year; rows outside that range are ignored. When
    needs_inflation is false it is not read, so it may be empty.

    Raises:
        ValueError: units is not positive.
        LevyChargeUnavailableError: an index in the range is missing, or the
            calculation date is before the base price took effect.
        LevyAmountTooLargeError: the provisional, original base or
            inflation-adjusted amount exceeds MAX_LEVY_AMOUNT, so it could not be stored in the backend.
            Intermediate lines are not sent to nrf-backend, so they are not
            limited.
        LevyUnitsTooLargeError: units exceeds MAX_LEVY_UNITS, so it could not
            be stored in the backend.
    """
    if units <= 0:
        msg = f"units must be positive, got {units}"
        raise ValueError(msg)

    provisional = charge.base_price_per_unit * units
    years = (
        _year_indices(charge, calculation_date, indices)
        if needs_inflation(charge, units)
        else []
    )
    base = years[0] if years else None
    # Divide last so the unrounded amount stays exact whenever it can be.
    unrounded = [provisional * i.cil_index / base.cil_index for i in years]

    # Only the amounts sent to nrf-backend are limited. Checked before rounding
    # so the error names the unrounded amount. Numeric(10, 4) indices bound any
    # ratio to under 1e10, so every line stays within quantize's precision.
    original_base = charge.original_base_price_per_unit * units
    largest = max([provisional, original_base, *unrounded[-1:]])
    if largest >= _FIRST_AMOUNT_TOO_LARGE:
        raise LevyAmountTooLargeError(largest, units, charge.edp_id)
    if units > MAX_LEVY_UNITS:
        raise LevyUnitsTooLargeError(units, charge.edp_id)

    lines = [
        LevyYearLine(
            year=i.charging_year,
            index_id=i.id,
            index_value=i.cil_index,
            amount=round_gbp(amount),
        )
        for i, amount in zip(years, unrounded, strict=True)
    ]
    return LevyCalculation(
        edp_id=charge.edp_id,
        edp_start_date=charge.edp_start_date,
        calculator_version=LEVY_CALCULATOR_VERSION,
        units=units,
        base_price_per_unit=charge.base_price_per_unit,
        base_price_effective_from=charge.base_price_effective_from,
        original_base_price_per_unit=charge.original_base_price_per_unit,
        provisional_amount=round_gbp(provisional),
        original_base_amount=round_gbp(original_base),
        inflation_adjusted_amount=lines[-1].amount if lines else _ZERO_GBP,
        yearly_lines=lines,
    )


def _year_indices(
    charge: LevyChargeRow, calculation_date: date, indices: Sequence[LevyIndexRow]
) -> list[LevyIndexRow]:
    """The index row for each year from the base year to the calculation year.

    Raises:
        LevyChargeUnavailableError: the range is empty or a year has no row.
    """
    first, last = charge.base_price_effective_from.year, calculation_date.year
    if last < first:
        msg = f"calculation year {last} is before the base price year {first}"
        raise LevyChargeUnavailableError(msg)
    by_year = {i.charging_year: i for i in indices}
    missing = [y for y in range(first, last + 1) if y not in by_year]
    if missing:
        msg = f"Missing RICS CIL inflation index for years {missing}"
        raise LevyChargeUnavailableError(msg)
    return [by_year[y] for y in range(first, last + 1)]
