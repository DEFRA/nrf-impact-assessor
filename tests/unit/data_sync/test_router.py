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


def test_trigger_rejects_empty_tables(client):
    with patch("app.data_sync.router.run_data_sync"):
        resp = client.post(
            "/admin/data-sync",
            headers={"X-Data-Sync-Token": "secret"},
            json={"data_version": "v1", "tables": {}},
        )
    assert resp.status_code == 422
