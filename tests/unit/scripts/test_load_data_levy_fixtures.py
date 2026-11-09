"""edps/levy_inflation_index CSV fixtures parse into their models.

These fixtures only ever load in fixtures_dir mode (see load_data.py); the
tables are finance-confirmed data in production, seeded solely by migration.
"""

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from load_data import SpatialDataLoader, _edp_from_row, _levy_index_from_row
from sqlalchemy.exc import IntegrityError

from app.models.db import Edp


def test_edp_from_row_parses_types():
    row = {"id": "1", "name": "Norfolk EDP", "start_date": "2026-01-01"}

    edp = _edp_from_row(row)

    assert edp.id == 1
    assert edp.name == "Norfolk EDP"
    assert edp.start_date == date(2026, 1, 1)


def test_levy_index_from_row_parses_types():
    row = {"charging_year": "2026", "cil_index": "400", "index_factor": "1.0000"}

    index = _levy_index_from_row(row)

    assert index.charging_year == 2026
    assert index.cil_index == Decimal("400")
    assert index.index_factor == Decimal("1.0000")


def _csv(tmp_path, edp_id: str):
    path = tmp_path / "edps.csv"
    path.write_text(f"id,name,start_date\n{edp_id},Norfolk EDP,2026-01-01\n")
    return path


def test_removing_an_edp_with_base_prices_explains_the_fk_failure(tmp_path):
    # A base price holds a foreign key to the EDP, so rekeying the row (a
    # delete plus an insert) fails at the delete flush.
    referenced = _edp_from_row(
        {"id": "1", "name": "Norfolk EDP", "start_date": "2026-01-01"}
    )
    session = MagicMock()
    session.scalars.return_value = [referenced]
    session.flush.side_effect = IntegrityError("DELETE", {}, Exception("fk"))
    repository = MagicMock()
    repository.session.return_value.__enter__.return_value = session
    loader = SpatialDataLoader.__new__(SpatialDataLoader)
    loader.repository = repository
    csv_path = _csv(tmp_path, edp_id="2")

    with pytest.raises(RuntimeError, match=r"edps.csv \(id=1\)"):
        loader._upsert_csv_table(csv_path, Edp, _edp_from_row, key=("id",))
