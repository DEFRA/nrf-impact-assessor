"""Integration tests for POST /admin/data-sync/rollback."""

import threading
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import text

pytestmark = pytest.mark.integration


@pytest.fixture
def client():
    from app.data_sync import router as router_module

    app = FastAPI()
    app.include_router(router_module.router)
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture(autouse=True)
def reset_engine_singleton():
    from app.data_sync import router as router_module

    router_module._engine = None
    yield
    router_module._engine = None


@pytest.fixture(autouse=True)
def _env(monkeypatch, test_engine):
    monkeypatch.setenv("DATA_SYNC_AUTH_TOKEN", "test-token")
    monkeypatch.setenv("DB_IAM_AUTHENTICATION", "false")
    monkeypatch.setenv("DB_DATABASE", "test_nrf_impact")
    # router.py's engine is created lazily and cached at module level; point
    # it at the test database for this process by reusing test_engine's DSN
    # via the same env vars DatabaseSettings reads.
    yield
    with test_engine.begin() as conn:
        conn.execute(text("DELETE FROM public.data_rollback_event"))
        conn.execute(text("DELETE FROM public.data_active_version"))
        conn.execute(text("DELETE FROM public.data_load_history"))
        conn.execute(text("DELETE FROM public.data_sync_run"))
        conn.execute(text("TRUNCATE public.nn_catchments"))


def _seed_two_versions(test_engine, run_id):
    """Two nn_catchments versions + a successful run/history row, mimicking
    what two real reloads would leave behind, without spinning up S3/psql.
    """
    with test_engine.begin() as conn:
        for v in (1, 2):
            conn.execute(
                text(
                    "INSERT INTO public.nn_catchments "
                    "(id, version, geometry, name, attributes) VALUES "
                    "(gen_random_uuid(), :v, "
                    "ST_GeomFromText('POLYGON((0 0,0 1,1 1,1 0,0 0))', 27700), 'x', '{}')"
                ),
                {"v": v},
            )
        conn.execute(
            text(
                "INSERT INTO public.data_sync_run (id, status, data_version) "
                "VALUES (:id, 'success', 'v2')"
            ),
            {"id": str(run_id)},
        )
        conn.execute(
            text(
                "INSERT INTO public.data_load_history "
                "(id, run_id, table_name, s3_key, etag, data_version, status) "
                "VALUES (gen_random_uuid(), :run_id, 'nn_catchments', 'k', 'e', 'v2', 'success')"
            ),
            {"run_id": str(run_id)},
        )
        conn.execute(
            text(
                "INSERT INTO public.data_active_version (table_name, active_version) "
                "VALUES ('nn_catchments', 2)"
            )
        )


def test_rollback_defaults_to_last_run_tables(client, test_engine, monkeypatch):
    monkeypatch.setenv("DATA_SYNC_TABLES", '["nn_catchments"]')
    run_id = uuid4()
    _seed_two_versions(test_engine, run_id)

    resp = client.post(
        "/admin/data-sync/rollback", headers={"X-Data-Sync-Token": "test-token"}
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["rolled_back"] == {"nn_catchments": {"from": 2, "to": 1}}
    assert body["skipped"] == {}

    with test_engine.connect() as conn:
        active = conn.execute(
            text(
                "SELECT active_version FROM public.data_active_version "
                "WHERE table_name = 'nn_catchments'"
            )
        ).scalar()
        events = conn.execute(
            text("SELECT count(*) FROM public.data_rollback_event")
        ).scalar()
    assert active == 1
    assert events == 1


def test_rollback_explicit_table_list_rejects_non_allow_listed_table(
    client, test_engine, monkeypatch
):
    monkeypatch.setenv("DATA_SYNC_TABLES", '["nn_catchments"]')
    resp = client.post(
        "/admin/data-sync/rollback",
        json={"tables": ["not_a_real_table"]},
        headers={"X-Data-Sync-Token": "test-token"},
    )
    assert resp.status_code == 400


def test_rollback_reports_skipped_when_no_previous_version(
    client, test_engine, monkeypatch
):
    monkeypatch.setenv("DATA_SYNC_TABLES", '["nn_catchments"]')
    run_id = uuid4()
    with test_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO public.nn_catchments "
                "(id, version, geometry, name, attributes) VALUES "
                "(gen_random_uuid(), 1, "
                "ST_GeomFromText('POLYGON((0 0,0 1,1 1,1 0,0 0))', 27700), 'x', '{}')"
            )
        )
        conn.execute(
            text(
                "INSERT INTO public.data_sync_run (id, status, data_version) "
                "VALUES (:id, 'success', 'v1')"
            ),
            {"id": str(run_id)},
        )
        conn.execute(
            text(
                "INSERT INTO public.data_load_history "
                "(id, run_id, table_name, s3_key, etag, data_version, status) "
                "VALUES (gen_random_uuid(), :run_id, 'nn_catchments', 'k', 'e', 'v1', 'success')"
            ),
            {"run_id": str(run_id)},
        )

    resp = client.post(
        "/admin/data-sync/rollback", headers={"X-Data-Sync-Token": "test-token"}
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["rolled_back"] == {}
    assert "nn_catchments" in body["skipped"]


def test_rollback_rejects_while_run_in_progress(client, test_engine, monkeypatch):
    monkeypatch.setenv("DATA_SYNC_TABLES", '["nn_catchments"]')
    with test_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO public.data_sync_run (id, status) VALUES (gen_random_uuid(), 'running')"
            )
        )

    resp = client.post(
        "/admin/data-sync/rollback", headers={"X-Data-Sync-Token": "test-token"}
    )
    assert resp.status_code == 409


def test_rollback_requires_token(client, test_engine):
    resp = client.post("/admin/data-sync/rollback")
    assert resp.status_code == 401


def test_rollback_blocks_until_reload_advisory_lock_is_released(
    client, test_engine, monkeypatch
):
    """Proves the fix for the rollback/reload race (finding 1): the rollback
    endpoint must share the same Postgres advisory lock a reload holds for
    its entire duration, not just a point-in-time 'no run is running' check.

    A real reload race is hard to reproduce deterministically end-to-end (it
    depends on timing between a reload's promote step and its retention
    cleanup), so instead this test proves the mechanism directly: it holds
    the advisory lock on a separate connection (simulating an in-flight
    reload) and confirms the rollback endpoint call — issued concurrently on
    a background thread — is genuinely blocked for as long as the lock is
    held, then proceeds and succeeds as soon as the lock is released. That is
    exactly the property the fix relies on to close the race.
    """
    from app.config import DataSyncConfig

    monkeypatch.setenv("DATA_SYNC_TABLES", '["nn_catchments"]')
    run_id = uuid4()
    _seed_two_versions(test_engine, run_id)

    lock_key = DataSyncConfig().lock_key

    # Simulate an in-flight reload holding the advisory lock for its duration.
    lock_conn = test_engine.connect()
    lock_conn.execution_options(isolation_level="AUTOCOMMIT")
    lock_conn.execute(text("SELECT pg_advisory_lock(:k)"), {"k": lock_key})

    result: dict = {}

    def call_rollback() -> None:
        result["resp"] = client.post(
            "/admin/data-sync/rollback", headers={"X-Data-Sync-Token": "test-token"}
        )

    thread = threading.Thread(target=call_rollback)
    try:
        thread.start()
        thread.join(timeout=1.0)
        assert thread.is_alive(), (
            "rollback endpoint returned before the advisory lock was released "
            "— it is not actually blocking on the reload's lock"
        )
    finally:
        lock_conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": lock_key})
        lock_conn.close()

    thread.join(timeout=5.0)
    assert not thread.is_alive(), "rollback endpoint never returned after lock release"

    resp = result["resp"]
    assert resp.status_code == 200
    assert resp.json()["rolled_back"] == {"nn_catchments": {"from": 2, "to": 1}}
