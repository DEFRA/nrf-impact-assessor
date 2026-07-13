import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.data_sync.active_version import (
    get_active_version,
    rollback_table,
    set_active_version,
)

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def _clean(test_engine):
    yield
    with test_engine.begin() as conn:
        conn.execute(text("DELETE FROM public.data_active_version"))
        conn.execute(text("TRUNCATE public.nn_catchments"))


def _insert_nn_version(session: Session, version: int) -> None:
    session.execute(
        text(
            "INSERT INTO public.nn_catchments "
            "(id, version, geometry, name, attributes) VALUES "
            "(gen_random_uuid(), :v, ST_GeomFromText('POLYGON((0 0,0 1,1 1,1 0,0 0))', 27700), "
            "'x', '{}')"
        ),
        {"v": version},
    )
    session.commit()


def test_get_active_version_falls_back_to_max_when_no_pointer_row(test_engine):
    with Session(bind=test_engine) as session:
        _insert_nn_version(session, 1)
        _insert_nn_version(session, 2)
        assert get_active_version(session, "nn_catchments") == 2


def test_set_active_version_upserts(test_engine):
    with Session(bind=test_engine) as session:
        _insert_nn_version(session, 1)
        set_active_version(session, "nn_catchments", 1)
        session.commit()
        assert get_active_version(session, "nn_catchments") == 1

        set_active_version(session, "nn_catchments", 5)
        session.commit()
        assert get_active_version(session, "nn_catchments") == 5


def test_rollback_table_decrements_pointer(test_engine):
    with Session(bind=test_engine) as session:
        _insert_nn_version(session, 1)
        _insert_nn_version(session, 2)
        set_active_version(session, "nn_catchments", 2)
        session.commit()

        from_v, to_v = rollback_table(session, "nn_catchments")
        session.commit()

        assert (from_v, to_v) == (2, 1)
        assert get_active_version(session, "nn_catchments") == 1


def test_rollback_table_raises_when_no_previous_version_retained(test_engine):
    with Session(bind=test_engine) as session:
        _insert_nn_version(session, 1)
        set_active_version(session, "nn_catchments", 1)
        session.commit()

        with pytest.raises(ValueError, match="no retained previous version"):
            rollback_table(session, "nn_catchments")


def test_rollback_table_rejects_unsafe_identifier(test_engine):
    with (
        Session(bind=test_engine) as session,
        pytest.raises(ValueError, match="identifier"),
    ):
        rollback_table(session, "nn_catchments; DROP TABLE users; --")
