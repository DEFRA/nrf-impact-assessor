"""A `running` row whose owner died must not block reloads forever.

`_finish` is the only thing that clears `status='running'`, and it runs in a
Python `finally` path that a SIGKILL (deploy, ECS task replacement, OOM) skips
entirely. The row then blocks every subsequent POST via the partial unique
index, permanently, with no API-level remedy.

Liveness is keyed off the advisory lock a run holds for its whole duration
(service.py::run_data_sync), because Postgres drops that lock when the owning
connection dies. Lock held => owner alive => 409. No lock and past the grace
period => orphaned => take over.

Two locks are checked, not one. The parent's key covers the Python process; the
psql child holds restore_lock_key(key) for the life of its transaction
(restore.py::begin_sql) because it runs on its own connection and can outlive a
SIGKILLed parent. Reclaiming on the parent's key alone would let a second run
start alongside an orphaned psql still holding locks on the live tables.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import patch
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import text

pytestmark = pytest.mark.integration

_BODY = {"tables": {"nn_catchments": {"key": "nn.sql.gz", "version": "v1"}}}
_HEADERS = {"X-Data-Sync-Token": "test-token"}


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
    yield
    with test_engine.begin() as conn:
        conn.execute(text("DELETE FROM public.data_sync_run"))


def _insert_running(test_engine, *, age: timedelta) -> str:
    """A `running` row started `age` ago, as a crashed run would leave behind."""
    run_id = uuid4()
    with test_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO public.data_sync_run (id, status, started_at) "
                "VALUES (:id, 'running', :started)"
            ),
            {"id": str(run_id), "started": datetime.now(UTC) - age},
        )
    return str(run_id)


def _status(test_engine, run_id: str) -> tuple[str, str | None]:
    with test_engine.connect() as conn:
        return conn.execute(
            text("SELECT status, error FROM public.data_sync_run WHERE id = :id"),
            {"id": run_id},
        ).one()


def test_orphaned_run_is_taken_over(client, test_engine):
    """No lock held and past the grace period: the reload proceeds."""
    orphan = _insert_running(test_engine, age=timedelta(hours=3))

    with patch("app.data_sync.router.run_data_sync"):
        resp = client.post("/admin/data-sync", headers=_HEADERS, json=_BODY)

    assert resp.status_code == 202
    assert resp.json()["run_id"] != orphan

    status, error = _status(test_engine, orphan)
    assert status == "failed"
    assert "orphaned" in (error or "")


def test_live_run_still_conflicts(client, test_engine):
    """Lock held by a live owner: 409, however old the row is.

    Guards the failure mode a plain age-based lease would have: a genuinely
    long restore must never get a second concurrent run.
    """
    from app.config import DataSyncConfig

    key = DataSyncConfig().lock_key
    running = _insert_running(test_engine, age=timedelta(hours=3))

    with test_engine.connect() as lock_conn:
        lock_conn.execution_options(isolation_level="AUTOCOMMIT")
        lock_conn.execute(text("SELECT pg_advisory_lock(:k)"), {"k": key})
        try:
            with patch("app.data_sync.router.run_data_sync"):
                resp = client.post("/admin/data-sync", headers=_HEADERS, json=_BODY)
        finally:
            lock_conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": key})

    assert resp.status_code == 409
    assert _status(test_engine, running)[0] == "running"


def test_orphaned_parent_with_live_psql_child_still_conflicts(client, test_engine):
    """The parent is gone but its psql child is still in its transaction.

    This is the reclamation hole: the parent's session lock vanishes with the
    parent, so on that key alone the row looks orphaned and a second run would
    start while the child still holds locks on the live tables. The child's own
    key is what keeps the 409.
    """
    from app.config import DataSyncConfig
    from app.data_sync.restore import restore_lock_key

    child_key = restore_lock_key(DataSyncConfig().lock_key)
    running = _insert_running(test_engine, age=timedelta(hours=3))

    # A transaction-scoped lock, exactly as begin_sql takes it — and note no
    # parent lock is held anywhere in this test.
    with test_engine.begin() as child_conn:
        child_conn.execute(text("SELECT pg_advisory_xact_lock(:k)"), {"k": child_key})
        with patch("app.data_sync.router.run_data_sync"):
            resp = client.post("/admin/data-sync", headers=_HEADERS, json=_BODY)

    assert resp.status_code == 409
    assert _status(test_engine, running)[0] == "running"


def test_run_is_reclaimed_once_the_psql_child_transaction_ends(client, test_engine):
    """The child's lock is transaction-scoped, so it cannot wedge the slot.

    Counterpart to the test above: once psql's transaction ends the lock goes
    with it, and the orphaned row is reclaimable again. Without this, a crash
    that left the child key held would be indistinguishable from the permanent
    block the whole mechanism exists to prevent.
    """
    from app.config import DataSyncConfig
    from app.data_sync.restore import restore_lock_key

    child_key = restore_lock_key(DataSyncConfig().lock_key)
    orphan = _insert_running(test_engine, age=timedelta(hours=3))

    with test_engine.begin() as child_conn:
        child_conn.execute(text("SELECT pg_advisory_xact_lock(:k)"), {"k": child_key})
    # Transaction closed: the lock is released.

    with patch("app.data_sync.router.run_data_sync"):
        resp = client.post("/admin/data-sync", headers=_HEADERS, json=_BODY)

    assert resp.status_code == 202
    assert _status(test_engine, orphan)[0] == "failed"


def test_young_run_without_lock_still_conflicts(client, test_engine):
    """Inside the grace period: 409 even with no lock held.

    Covers the gap between `_create_run` inserting the row and the background
    task acquiring the lock, during which a live run legitimately holds no lock.
    """
    running = _insert_running(test_engine, age=timedelta(seconds=1))

    with patch("app.data_sync.router.run_data_sync"):
        resp = client.post("/admin/data-sync", headers=_HEADERS, json=_BODY)

    assert resp.status_code == 409
    assert _status(test_engine, running)[0] == "running"
