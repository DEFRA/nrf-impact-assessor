"""levy_inflation_index CSV fixture parses into its model.

This fixture only ever loads in fixtures_dir mode (see load_data.py); the
table is finance-confirmed data in production, seeded solely by migration.
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from load_data import SpatialDataLoader, _levy_index_from_row
from sqlalchemy.exc import IntegrityError

from app.models.db import LevyInflationIndex


def test_levy_index_from_row_parses_types():
    row = {"charging_year": "2026", "cil_index": "400", "index_factor": "1.0000"}

    index = _levy_index_from_row(row)

    assert index.charging_year == 2026
    assert index.cil_index == Decimal("400")
    assert index.index_factor == Decimal("1.0000")


def _csv(tmp_path, charging_year: str):
    path = tmp_path / "levy_inflation_index.csv"
    path.write_text(
        f"charging_year,cil_index,index_factor\n{charging_year},400,1.0000\n"
    )
    return path


def test_removing_a_priced_index_explains_the_fk_failure(tmp_path):
    # A quote priced from the 2026 row holds a RESTRICT FK to it, so rekeying
    # the row (a delete plus an insert) fails at the delete flush.
    priced = _levy_index_from_row(
        {"charging_year": "2026", "cil_index": "400", "index_factor": "1.0000"}
    )
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
