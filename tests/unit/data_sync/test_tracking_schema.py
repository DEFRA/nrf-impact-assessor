"""Unit-level checks on the data-sync tracking ORM models."""

from app.models.db import DataLoadHistory


def test_data_load_history_has_row_version_column():
    col = DataLoadHistory.__table__.c.row_version
    assert col.nullable is True
    assert str(col.type) in {"INTEGER", "INT"}
