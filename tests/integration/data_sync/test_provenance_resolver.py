"""Integration tests for resolve_active_provenance (DM-3), per-table.

Provenance keys off each table's active-version pointer joined to the history
row at that row_version, so it is rollback-accurate. These tests seed
data_load_history + data_active_version directly to exercise that join.
"""

from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.data_sync.service import resolve_active_provenance

pytestmark = pytest.mark.integration


def _history(conn, run_id, table, row_version, data_version):
    conn.execute(
        text(
            "INSERT INTO public.data_load_history "
            "(id, run_id, table_name, s3_key, etag, data_version, status, row_version) "
            "VALUES (gen_random_uuid(), :run, :t, 'k', 'e', :dv, 'success', :rv)"
        ),
        {"run": str(run_id), "t": table, "dv": data_version, "rv": row_version},
    )


def _active(conn, table, version):
    conn.execute(
        text(
            "INSERT INTO public.data_active_version (table_name, active_version) "
            "VALUES (:t, :v) "
            "ON CONFLICT (table_name) DO UPDATE SET active_version = EXCLUDED.active_version"
        ),
        {"t": table, "v": version},
    )


@pytest.fixture(autouse=True)
def _clean(test_engine):
    yield
    with test_engine.begin() as conn:
        conn.execute(text("DELETE FROM public.data_load_history"))
        conn.execute(text("DELETE FROM public.data_active_version"))
        conn.execute(text("DELETE FROM public.data_sync_run"))


def test_reports_each_table_at_its_active_version(test_engine):
    run_a, run_b = uuid4(), uuid4()
    with test_engine.begin() as conn:
        for rid in (run_a, run_b):
            conn.execute(
                text(
                    "INSERT INTO public.data_sync_run (id, status) "
                    "VALUES (:id, 'success')"
                ),
                {"id": str(rid)},
            )
        # nn_catchments active at version 1 (from run_a, data_version A).
        _history(conn, run_a, "nn_catchments", 1, "A")
        _active(conn, "nn_catchments", 1)
        # lpa_boundaries active at version 2 (from run_b, data_version B).
        _history(conn, run_b, "lpa_boundaries", 1, "A")
        _history(conn, run_b, "lpa_boundaries", 2, "B")
        _active(conn, "lpa_boundaries", 2)

    with Session(bind=test_engine) as session:
        prov = resolve_active_provenance(session)

    assert prov.tables["nn_catchments"].data_version == "A"
    assert prov.tables["nn_catchments"].data_sync_run_id == run_a
    assert prov.tables["lpa_boundaries"].data_version == "B"
    assert prov.tables["lpa_boundaries"].data_sync_run_id == run_b


def test_rollback_accurate_reflects_active_pointer_not_latest_load(test_engine):
    """After a rollback the active pointer moves back; provenance must report the
    now-active version, not the most recently loaded one."""
    run_id = uuid4()
    with test_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO public.data_sync_run (id, status) VALUES (:id, 'success')"
            ),
            {"id": str(run_id)},
        )
        _history(conn, run_id, "nn_catchments", 1, "A")
        _history(conn, run_id, "nn_catchments", 2, "B")
        # Rolled back: active pointer is at version 1 even though 2 was loaded.
        _active(conn, "nn_catchments", 1)

    with Session(bind=test_engine) as session:
        prov = resolve_active_provenance(session)

    assert prov.tables["nn_catchments"].data_version == "A"


def test_empty_map_when_nothing_loaded(test_engine):
    with test_engine.begin() as conn:
        for table in ("nn_catchments", "lpa_boundaries"):
            conn.execute(text(f"TRUNCATE public.{table} CASCADE"))
        conn.execute(text("DELETE FROM public.data_load_history"))
        conn.execute(text("DELETE FROM public.data_active_version"))

    with Session(bind=test_engine) as session:
        prov = resolve_active_provenance(session)

    assert prov.tables == {}
