"""Schema-level checks for the data-sync tracking tables."""

import pytest
from sqlalchemy import inspect, text

pytestmark = pytest.mark.integration


def test_data_sync_run_table_exists(test_engine):
    insp = inspect(test_engine)
    cols = {c["name"] for c in insp.get_columns("data_sync_run", schema="public")}
    assert {
        "id",
        "status",
        "data_version",
        "forced",
        "started_at",
        "finished_at",
        "error",
    } <= cols


def test_data_load_history_table_exists(test_engine):
    insp = inspect(test_engine)
    cols = {c["name"] for c in insp.get_columns("data_load_history", schema="public")}
    assert {
        "id",
        "run_id",
        "table_name",
        "s3_key",
        "etag",
        "data_version",
        "status",
        "loaded_at",
    } <= cols


def test_only_one_running_run_allowed(test_engine):
    """The partial unique index must reject a second 'running' row."""
    from uuid import uuid4

    import pytest
    from sqlalchemy.exc import IntegrityError

    with test_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO public.data_sync_run (id, status) VALUES (:id, 'running')"
            ),
            {"id": str(uuid4())},
        )
    with pytest.raises(IntegrityError):  # noqa: PT012, SIM117
        with test_engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO public.data_sync_run (id, status) "
                    "VALUES (:id, 'running')"
                ),
                {"id": str(uuid4())},
            )
    # cleanup so other tests start clean
    with test_engine.begin() as conn:
        conn.execute(text("DELETE FROM public.data_sync_run"))
