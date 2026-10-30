"""Nature restoration levy calculation.

The base charge per unit is a positive amount in pounds and pence: the
levy_charges table rejects anything else (ck_levy_charges_base_charge_per_unit),
so provisional levy = units x base_charge_per_unit is exact.

Inflation adjustment: the charging year is the calendar year. If
calculation_date falls in the EDP's own charging year (edp_start_date's
year), the base charge is used unadjusted. Otherwise it is scaled by the
ratio of the RICS CIL Index for the calculation year to the index for the
EDP's charging year, and the inflated total (units x base charge x ratio)
is rounded to 2 dp. That is the only rounding step that changes a value.
It is half-up: the third decimal 0-4 rounds down, 5-9 rounds up. Python's
built-in round() is banker's rounding, so it is never used here.
"""

from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from typing import Protocol
from uuid import UUID

from pydantic import BaseModel, Field

LEVY_CALCULATOR_VERSION = 1

_PENNY = Decimal("0.01")
# The largest amount audit_levy_calculations' numeric(12,2) columns hold.
MAX_LEVY_AMOUNT = Decimal("9999999999.99")
# The smallest unrounded amount that rounds half-up past MAX_LEVY_AMOUNT.
_FIRST_AMOUNT_TOO_LARGE = MAX_LEVY_AMOUNT + _PENNY / 2
# The most units audit_levy_calculations' integer units column holds. A cheap
# enough charge prices more units than this within MAX_LEVY_AMOUNT.
MAX_LEVY_UNITS = 2**31 - 1


class LevyChargeUnavailableError(RuntimeError):
    """The data needed to calculate the levy for a job is missing.

    Raised out of process_job so the consumer leaves the SQS message on the
    queue (it ages to the DLQ), no PATCH callback is sent, and therefore no
    quote email goes out with an assumed or default value.
    """


class LevyAmountTooLargeError(LevyChargeUnavailableError):
    """The levy exceeds MAX_LEVY_AMOUNT, so it cannot be recorded for audit.

    Carries the amount and units so the recalculate endpoint can tell the
    caller why, unlike the other reasons a levy is unavailable.
    """

    def __init__(self, amount: Decimal, units: int, edp_id: int) -> None:
        self.amount = amount
        self.units = units
        super().__init__(
            f"levy amount {amount} for edp_id={edp_id}, units={units} "
            f"exceeds the {MAX_LEVY_AMOUNT} an audit record holds"
        )


class LevyUnitsTooLargeError(LevyChargeUnavailableError):
    """units exceeds MAX_LEVY_UNITS, so it cannot be recorded for audit.

    Carries the units so the recalculate endpoint can tell the caller why.
    """

    def __init__(self, units: int, edp_id: int) -> None:
        self.units = units
        super().__init__(
            f"units={units} for edp_id={edp_id} exceeds the {MAX_LEVY_UNITS} "
            "an audit record holds"
        )


class LevyChargeRow(Protocol):
    """The charge fields the calculation reads.

    Structural rather than the LevyCharge ORM model, so this module stays free
    of database imports and a test can pass any object with these attributes.
    """

    id: UUID
    edp_id: int
    edp_name: str
    edp_start_date: date
    base_charge_per_unit: Decimal


class LevyIndexRow(Protocol):
    """The RICS CIL Index fields the calculation reads, structural as above."""

    id: UUID
    index_factor: Decimal


class LevyCalculation(BaseModel):
    """One levy calculation, carrying every audit field.

    The *_id fields name the rows the inputs came from, so the audit record can
    tell a later correction to a row from the wrong row being picked up. The
    index fields are None when no inflation step applied.
    """

    levy_charge_id: UUID
    edp_id: int
    edp_name: str
    edp_start_date: date
    calculation_date: date
    calculator_version: int
    units: int = Field(gt=0)
    base_charge_per_unit: Decimal
    provisional_amount: Decimal
    inflation_adjusted_amount: Decimal
    edp_start_year_index_id: UUID | None = None
    edp_start_year_index_factor: Decimal | None = None
    calculation_year_index_id: UUID | None = None
    calculation_year_index_factor: Decimal | None = None


def round_gbp(value: Decimal) -> Decimal:
    """Round to 2 decimal places, half-up, keeping a 2-place scale."""
    return value.quantize(_PENNY, rounding=ROUND_HALF_UP)


def calculate_levy(
    charge: LevyChargeRow,
    units: int,
    calculation_date: date,
    edp_start_year_index: LevyIndexRow | None = None,
    calculation_year_index: LevyIndexRow | None = None,
    *,
    apply_inflation: bool = True,
) -> LevyCalculation:
    """Calculate the provisional and inflation-adjusted levy for one EDP.

    Args:
        charge: A levy charge row (LevyCharge) with id, edp_id, edp_name,
            edp_start_date and base_charge_per_unit (Decimal, pounds and
            pence, at most 2 dp).
        units: Number of dwellings; must be positive.
        calculation_date: The date the quote is calculated on.
        edp_start_year_index: RICS CIL Index row (LevyInflationIndex) for
            the charging year the EDP was published (edp_start_date's year).
            Only required when calculation_date falls in a different charging
            year.
        calculation_year_index: RICS CIL Index row for calculation_date's
            charging year. Only required when calculation_date falls in a
            different charging year than the EDP's publication.
        apply_inflation: False skips the inflation step whatever the year,
            so the indices are neither needed nor recorded and the
            inflation-adjusted amount is the provisional amount.

    Raises:
        ValueError: units is not a positive integer.
        LevyChargeUnavailableError: inflation applies, calculation_date falls
            in a different charging year than the EDP's publication and
            either index is missing.
        LevyAmountTooLargeError: either amount exceeds MAX_LEVY_AMOUNT, so
            it could not be recorded for audit. A LevyChargeUnavailableError.
        LevyUnitsTooLargeError: units exceeds MAX_LEVY_UNITS, so it could not
            be recorded for audit. A LevyChargeUnavailableError.
    """
    if units <= 0:
        msg = f"units must be positive, got {units}"
        raise ValueError(msg)

    indices = _applied_indices(
        charge,
        calculation_date,
        edp_start_year_index,
        calculation_year_index,
        apply_inflation=apply_inflation,
    )
    provisional = charge.base_charge_per_unit * units
    if indices is None:
        inflation_adjusted = provisional
    else:
        start_index, calc_index = indices
        # Divide last so the unrounded total stays exact whenever it can be.
        inflation_adjusted = (
            provisional * calc_index.index_factor / start_index.index_factor
        )

    # Checked before rounding: quantize raises InvalidOperation once an amount
    # has more digits than the decimal context's precision, which a large
    # enough units would reach.
    largest = max(provisional, inflation_adjusted)
    if largest >= _FIRST_AMOUNT_TOO_LARGE:
        raise LevyAmountTooLargeError(largest, units, charge.edp_id)
    # After the amount check, so a units count too large on both counts reports
    # the amount, the limit a caller meets first at any realistic charge.
    if units > MAX_LEVY_UNITS:
        raise LevyUnitsTooLargeError(units, charge.edp_id)

    return LevyCalculation(
        levy_charge_id=charge.id,
        edp_id=charge.edp_id,
        edp_name=charge.edp_name,
        edp_start_date=charge.edp_start_date,
        calculation_date=calculation_date,
        calculator_version=LEVY_CALCULATOR_VERSION,
        units=units,
        base_charge_per_unit=charge.base_charge_per_unit,
        # Exact for pence, so this only sets a 2-place scale. Rounds half-up
        # like the inflated total if a caller bypasses the table's check.
        provisional_amount=round_gbp(provisional),
        inflation_adjusted_amount=round_gbp(inflation_adjusted),
        **_index_audit_fields(indices),
    )


def _applied_indices(
    charge: LevyChargeRow,
    calculation_date: date,
    edp_start_year_index: LevyIndexRow | None,
    calculation_year_index: LevyIndexRow | None,
    *,
    apply_inflation: bool,
) -> tuple[LevyIndexRow, LevyIndexRow] | None:
    """The (EDP start year, calculation year) indices to inflate by, or None
    when no inflation step applies.

    Raises:
        LevyChargeUnavailableError: inflation applies and either index is
            missing.
    """
    if not apply_inflation or calculation_date.year == charge.edp_start_date.year:
        return None
    if edp_start_year_index is None or calculation_year_index is None:
        msg = (
            "Missing RICS CIL inflation index for edp_start_year="
            f"{charge.edp_start_date.year} or calculation_year="
            f"{calculation_date.year}"
        )
        raise LevyChargeUnavailableError(msg)
    return edp_start_year_index, calculation_year_index


def _index_audit_fields(
    indices: tuple[LevyIndexRow, LevyIndexRow] | None,
) -> dict[str, UUID | Decimal]:
    """The audit record's index fields. Only indices that were actually applied
    go on it, so none when no inflation step applied."""
    if indices is None:
        return {}
    start_index, calc_index = indices
    return {
        "edp_start_year_index_id": start_index.id,
        "edp_start_year_index_factor": start_index.index_factor,
        "calculation_year_index_id": calc_index.id,
        "calculation_year_index_factor": calc_index.index_factor,
    }
