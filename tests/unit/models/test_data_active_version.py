from app.models.db import Base, DataActiveVersion, DataRollbackEvent


def test_data_active_version_table_name():
    assert DataActiveVersion.__tablename__ == "data_active_version"
    assert "table_name" in DataActiveVersion.__table__.columns
    assert "active_version" in DataActiveVersion.__table__.columns


def test_data_rollback_event_table_name():
    assert DataRollbackEvent.__tablename__ == "data_rollback_event"
    cols = DataRollbackEvent.__table__.columns
    assert {"id", "table_name", "from_version", "to_version", "rolled_back_at"} <= set(
        cols.keys()
    )


def test_models_registered_on_base_metadata():
    assert "public.data_active_version" in Base.metadata.tables
    assert "public.data_rollback_event" in Base.metadata.tables
