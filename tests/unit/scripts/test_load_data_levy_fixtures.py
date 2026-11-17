"""levy_inflation_index CSV fixture parses into its model.

This fixture only ever loads in fixtures_dir mode (see load_data.py); the
table is finance-confirmed data in production, seeded solely by migration.
Every row carries the provenance columns (comment, staff) the admin CRUD
writes, so a local reload never nulls them.
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from load_data import SpatialDataLoader, _levy_index_from_row
from sqlalchemy.exc import IntegrityError

from app.models.db import LevyInflationIndex

ROW = {
    "charging_year": "2026",
    "cil_index": "400",
    "index_factor": "1.0000",
    "comment": "Seeded with the published RICS CIL Index values",
    "staff_id": "system",
    "staff_full_name": "system",
    "staff_email": "system@nrf.invalid",
}


def test_levy_index_from_row_parses_types():
    index = _levy_index_from_row(ROW)

    assert index.charging_year == 2026
    assert index.cil_index == Decimal("400")
    assert index.index_factor == Decimal("1.0000")
    assert index.comment == ROW["comment"]
    assert index.staff_email == ROW["staff_email"]


def _csv(tmp_path, charging_year: str):
    path = tmp_path / "levy_inflation_index.csv"
    path.write_text(
        "charging_year,cil_index,index_factor,comment,staff_id,staff_full_name,"
        f"staff_email\n{charging_year},400,1.0000,Seeded,system,system,"
        "system@nrf.invalid\n"
    )
    return path


def test_removing_a_priced_index_explains_the_fk_failure(tmp_path):
    # A quote priced from the 2026 row holds a RESTRICT FK to it, so rekeying
    # the row (a delete plus an insert) fails at the delete flush.
    priced = _levy_index_from_row(ROW)
    session = MagicMock()
    session.scalars.return_value = [priced]
    session.flush.side_effect = IntegrityError("DELETE", {}, Exception("fk"))
    repository = MagicMock()
    repository.session.return_value.__enter__.return_value = session
    loader = SpatialDataLoader.__new__(SpatialDataLoader)
    loader.repository = repository
    csv_path = _csv(tmp_path, "2027")

    with pytest.raises(RuntimeError, match=r"charging_year=2026"):
        loader._upsert_csv_table(
            csv_path,
            LevyInflationIndex,
            _levy_index_from_row,
            key=("charging_year",),
        )
