from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.dlq.models import DlqMessage, DlqPeekResult, DlqStats, RedriveTask
from app.dlq.service import HandleExpiredError


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("DLQ_ENABLED", "true")
    monkeypatch.setenv("DLQ_AUTH_TOKEN", "secret")
    from app.dlq import router as router_module

    router_module._service = None
    app = FastAPI()
    app.include_router(router_module.router)
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture(autouse=True)
def reset_service():
    from app.dlq import router as router_module

    router_module._service = None
    yield
    router_module._service = None


def _mock_service():
    svc = MagicMock()
    return svc


def test_stats_requires_token(client):
    resp = client.get("/admin/dlq")
    assert resp.status_code == 401


def test_stats_ok(client):
    svc = _mock_service()
    svc.stats.return_value = DlqStats(available=2, in_flight=0)
    with patch("app.dlq.router._get_service", return_value=svc):
        resp = client.get("/admin/dlq", headers={"x-dlq-token": "secret"})
    assert resp.status_code == 200
    assert resp.json() == {"available": 2, "in_flight": 0}


def test_bulk_redrive_requires_confirm(client):
    svc = _mock_service()
    with patch("app.dlq.router._get_service", return_value=svc):
        resp = client.post(
            "/admin/dlq/redrive",
            headers={"x-dlq-token": "secret"},
            json={"confirm": False},
        )
    assert resp.status_code == 400
    svc.redrive_all.assert_not_called()


def test_bulk_redrive_ok(client):
    svc = _mock_service()
    svc.redrive_all.return_value = RedriveTask(task_handle="th1", status="RUNNING")
    with patch("app.dlq.router._get_service", return_value=svc):
        resp = client.post(
            "/admin/dlq/redrive",
            headers={"x-dlq-token": "secret"},
            json={"confirm": True, "max_per_second": 5},
        )
    assert resp.status_code == 200
    assert resp.json()["task_handle"] == "th1"


def test_selective_redrive_returns_hash_and_audits(client, caplog):
    import hashlib

    svc = _mock_service()
    body = '{"reference": "NRF-1"}'
    svc.redrive_message.return_value = hashlib.sha256(body.encode()).hexdigest()
    with (
        patch("app.dlq.router._get_service", return_value=svc),
        caplog.at_level("INFO"),
    ):
        resp = client.post(
            "/admin/dlq/messages/redrive",
            headers={"x-dlq-token": "secret"},
            json={"receipt_handle": "rh1", "body": body},
        )
    assert resp.status_code == 200
    assert resp.json()["body_sha256"] == svc.redrive_message.return_value
    assert any("dlq admin action" in r.message for r in caplog.records)


def test_stale_handle_maps_to_409(client):
    svc = _mock_service()
    svc.redrive_message.side_effect = HandleExpiredError()
    with patch("app.dlq.router._get_service", return_value=svc):
        resp = client.post(
            "/admin/dlq/messages/redrive",
            headers={"x-dlq-token": "secret"},
            json={"receipt_handle": "stale", "body": "{}"},
        )
    assert resp.status_code == 409
    assert resp.json()["detail"]["code"] == "handle_expired"


def test_peek_full_body_with_truncated_preview(client):
    svc = _mock_service()
    svc.peek.return_value = DlqPeekResult(
        messages=[
            DlqMessage(
                message_id="m1",
                receipt_handle="rh1",
                body="0123456789",
                body_preview="0123",
                body_truncated=True,
                body_bytes=10,
                receive_count=1,
            )
        ],
        visibility_deadline="2026-07-06T00:00:00Z",
        hold_seconds=60,
    )
    with patch("app.dlq.router._get_service", return_value=svc):
        resp = client.get(
            "/admin/dlq/messages?limit=1", headers={"x-dlq-token": "secret"}
        )
    assert resp.status_code == 200
    msg = resp.json()["messages"][0]
    assert msg["body"] == "0123456789"  # full body present for redrive
    assert msg["body_preview"] == "0123"
    assert msg["body_truncated"] is True
