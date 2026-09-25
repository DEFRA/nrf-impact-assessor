"""Nature restoration levy calculation (NRF2-913).

Provisional levy = units x round_gbp(base_charge_per_unit), rounded again to 2 dp.
Rounding is half-up (scenario 3): the third decimal 0-4 rounds down, 5-9 rounds
up. Python's built-in round() is banker's rounding, so it is never used here.

Inflation adjustment: the charging year is the calendar year. If
calculation_date falls in the EDP's own charging year (edp_start_date's
year), the published base charge is used unadjusted. Otherwise it is scaled
by the ratio of the RICS CIL Index for the calculation year to the index for
the EDP's charging year, rounded half-up to 2 dp before multiplying by units,
same as the unadjusted charge.
"""

from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from typing import Protocol
from uuid import UUID

from pydantic import BaseModel, Field

LEVY_CALCULATOR_VERSION = 1

_PENNY = Decimal("0.01")


class LevyChargeUnavailableError(RuntimeError):
    """The data needed to calculate the levy for a job is missing.

    Raised out of process_job so the consumer leaves the SQS message on the
    queue (it ages to the DLQ), no PATCH callback is sent, and therefore no
    quote email goes out with an assumed or default value (scenario 5).
    """


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
    """One levy calculation, carrying every scenario 7 audit field.

    The *_id fields name the rows the inputs came from, so the audit record can
    tell a later correction to a row from the wrong row being picked up. The
    index fields and inflation_adjusted_charge_per_unit (the rounded per-unit
    charge the inflation-adjusted amount multiplies) are None when no
    inflation step applied.
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
    inflation_adjusted_charge_per_unit: Decimal | None = None
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
) -> LevyCalculation:
    """Calculate the provisional and inflation-adjusted levy for one EDP.

    Args:
        charge: A levy charge row (LevyCharge) with id, edp_id, edp_name,
            edp_start_date and base_charge_per_unit (Decimal, 4 dp).
        units: Number of dwellings; must be positive.
        calculation_date: The date the quote is calculated on.
        edp_start_year_index: RICS CIL Index row (LevyInflationIndex) for
            the charging year the EDP was published (edp_start_date's year).
            Only required when calculation_date falls in a different charging
            year.
        calculation_year_index: RICS CIL Index row for calculation_date's
            charging year. Only required when calculation_date falls in a
            different charging year than the EDP's publication.

    Raises:
        ValueError: units is not a positive integer.
        LevyChargeUnavailableError: calculation_date falls in a different
            charging year than the EDP's publication and either index is
            missing.
    """
    if units <= 0:
        msg = f"units must be positive, got {units}"
        raise ValueError(msg)

    rounded = round_gbp(charge.base_charge_per_unit)
    provisional = round_gbp(rounded * units)

    # Only indices that were actually applied go on the audit record.
    start_index: LevyIndexRow | None = None
    calc_index: LevyIndexRow | None = None
    indexed_per_unit: Decimal | None = None
    if calculation_date.year == charge.edp_start_date.year:
        inflation_adjusted = provisional
    else:
        if edp_start_year_index is None or calculation_year_index is None:
            msg = (
                "Missing RICS CIL inflation index for edp_start_year="
                f"{charge.edp_start_date.year} or calculation_year="
                f"{calculation_date.year}"
            )
            raise LevyChargeUnavailableError(msg)
        start_index = edp_start_year_index
        calc_index = calculation_year_index
        factor = calc_index.index_factor / start_index.index_factor
        indexed_per_unit = round_gbp(charge.base_charge_per_unit * factor)
        inflation_adjusted = round_gbp(indexed_per_unit * units)

    return LevyCalculation(
        levy_charge_id=charge.id,
        edp_id=charge.edp_id,
        edp_name=charge.edp_name,
        edp_start_date=charge.edp_start_date,
        calculation_date=calculation_date,
        calculator_version=LEVY_CALCULATOR_VERSION,
        units=units,
        base_charge_per_unit=charge.base_charge_per_unit,
        provisional_amount=provisional,
        inflation_adjusted_amount=inflation_adjusted,
        inflation_adjusted_charge_per_unit=indexed_per_unit,
        edp_start_year_index_id=start_index.id if start_index else None,
        edp_start_year_index_factor=start_index.index_factor if start_index else None,
        calculation_year_index_id=calc_index.id if calc_index else None,
        calculation_year_index_factor=calc_index.index_factor if calc_index else None,
    )
