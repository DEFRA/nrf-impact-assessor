"""levy_charges/levy_inflation_index CSV fixtures parse into their models.

These fixtures only ever load in fixtures_dir mode (see load_data.py); the
tables are finance-confirmed data in production, seeded solely by migration.
"""

from datetime import date
from decimal import Decimal

from load_data import _levy_charge_from_row, _levy_index_from_row


def test_levy_charge_from_row_parses_types():
    row = {
        "edp_id": "1",
        "edp_name": "Norfolk EDP",
        "edp_start_date": "2026-01-01",
        "charge_valid_from": "2026-01-01",
        "charge_valid_to": "2027-12-31",
        "base_charge_per_unit": "2193.6649",
    }

    charge = _levy_charge_from_row(row)

    assert charge.edp_id == 1
    assert charge.edp_name == "Norfolk EDP"
    assert charge.edp_start_date == date(2026, 1, 1)
    assert charge.charge_valid_from == date(2026, 1, 1)
    assert charge.charge_valid_to == date(2027, 12, 31)
    assert charge.base_charge_per_unit == Decimal("2193.6649")


def test_levy_index_from_row_parses_types():
    row = {"charging_year": "2026", "cil_index": "400", "index_factor": "1.0000"}

    index = _levy_index_from_row(row)

    assert index.charging_year == 2026
    assert index.cil_index == Decimal("400")
    assert index.index_factor == Decimal("1.0000")
