"""levy_charges/levy_inflation_index CSV fixtures parse into their models.

These fixtures only ever load in fixtures_dir mode (see load_data.py); the
tables are finance-confirmed data in production, seeded solely by migration.
"""

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from load_data import SpatialDataLoader, _levy_charge_from_row, _levy_index_from_row
from sqlalchemy.exc import IntegrityError

from app.models.db import LevyCharge


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


def _csv(tmp_path, charge_valid_from: str):
    path = tmp_path / "levy_charges.csv"
    path.write_text(
        "edp_id,edp_name,edp_start_date,charge_valid_from,charge_valid_to,"
        "base_charge_per_unit\n"
        f"1,Norfolk EDP,2026-01-01,{charge_valid_from},2027-12-31,2193.6649\n"
    )
    return path


def test_removing_a_priced_charge_explains_the_fk_failure(tmp_path):
    # A quote priced from the 2026-01-01 row holds a RESTRICT FK to it, so
    # rekeying the row (a delete plus an insert) fails at the delete flush.
    priced = _levy_charge_from_row(
        {
            "edp_id": "1",
            "edp_name": "Norfolk EDP",
            "edp_start_date": "2026-01-01",
            "charge_valid_from": "2026-01-01",
            "charge_valid_to": "2027-12-31",
            "base_charge_per_unit": "2193.6649",
        }
    )
    session = MagicMock()
    session.scalars.return_value = [priced]
    session.flush.side_effect = IntegrityError("DELETE", {}, Exception("fk"))
    repository = MagicMock()
    repository.session.return_value.__enter__.return_value = session
    loader = SpatialDataLoader.__new__(SpatialDataLoader)
    loader.repository = repository
    csv_path = _csv(tmp_path, "2026-04-01")

    with pytest.raises(RuntimeError, match=r"edp_id=1, charge_valid_from=2026-01-01"):
        loader._upsert_csv_table(
            csv_path,
            LevyCharge,
            _levy_charge_from_row,
            key=("edp_id", "charge_valid_from"),
        )
