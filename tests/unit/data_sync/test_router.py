from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("DATA_SYNC_ENABLED", "true")
    monkeypatch.setenv("DATA_SYNC_AUTH_TOKEN", "secret")
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


def test_trigger_requires_auth(client):
    resp = client.post("/admin/data-sync")
    assert resp.status_code == 401


_BODY = {"data_version": "v1", "tables": {"nn_catchments": "nn.sql.gz"}}


def test_trigger_returns_202_and_run_id(client):
    with (
        patch("app.data_sync.router._create_run") as create,
        patch("app.data_sync.router.run_data_sync"),
    ):
        run_id = uuid4()
        create.return_value = run_id
        resp = client.post(
            "/admin/data-sync",
            headers={"X-Data-Sync-Token": "secret"},
            json=_BODY,
        )
    assert resp.status_code == 202
    assert resp.json()["run_id"] == str(run_id)


def test_trigger_conflict_returns_409(client):
    from app.data_sync.router import RunInProgressError

    with patch("app.data_sync.router._create_run", side_effect=RunInProgressError()):
        resp = client.post(
            "/admin/data-sync",
            headers={"X-Data-Sync-Token": "secret"},
            json=_BODY,
        )
    assert resp.status_code == 409


def test_get_engine_created_once():
    from app.data_sync import router as router_module

    with patch("app.data_sync.router.create_db_engine") as create:
        first = router_module._get_engine()
        second = router_module._get_engine()
    assert first is second
    assert create.call_count == 1


def test_status_endpoint_reuses_engine(client):
    run = SimpleNamespace(
        id=uuid4(),
        status="success",
        data_version="v1",
        forced=False,
        started_at=None,
        finished_at=None,
        error=None,
    )
    with (
        patch("app.data_sync.router.create_db_engine") as create,
        patch("app.data_sync.router.Session") as session_cls,
    ):
        session_cls.return_value.__enter__.return_value.get.return_value = run
        headers = {"X-Data-Sync-Token": "secret"}
        first = client.get(f"/admin/data-sync/{run.id}", headers=headers)
        second = client.get(f"/admin/data-sync/{run.id}", headers=headers)
    assert first.status_code == second.status_code == 200
    assert create.call_count == 1
    create.return_value.dispose.assert_not_called()


def test_trigger_rejects_empty_tables(client):
    with patch("app.data_sync.router.run_data_sync"):
        resp = client.post(
            "/admin/data-sync",
            headers={"X-Data-Sync-Token": "secret"},
            json={"data_version": "v1", "tables": {}},
        )
    assert resp.status_code == 422
