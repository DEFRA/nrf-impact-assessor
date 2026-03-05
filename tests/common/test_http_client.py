import httpx

from app.common.http_client import hook_request_tracing
from app.common.tracing import ctx_trace_id


def mock_handler(request):
    request_id = request.headers.get("x-cdp-request-id", "")
    return httpx.Response(200, text=request_id)


def test_trace_id_missing():
    ctx_trace_id.set("")
    client = httpx.Client(
        event_hooks={"request": [hook_request_tracing]},
        transport=httpx.MockTransport(mock_handler),
    )
    resp = client.get("http://localhost:1234/test")
    assert resp.text == ""


def test_trace_id_set():
    ctx_trace_id.set("trace-id-value")
    client = httpx.Client(
        event_hooks={"request": [hook_request_tracing]},
        transport=httpx.MockTransport(mock_handler),
    )
    resp = client.get("http://localhost:1234/test")
    assert resp.text == "trace-id-value"


def test_create_client_with_proxy(monkeypatch):
    import importlib

    from pydantic import HttpUrl

    # Set http_proxy config before reloading http_client
    monkeypatch.setattr(
        "app.config.config.http_proxy", HttpUrl("http://proxy.example.com:8080")
    )

    # Reload the http_client module to trigger creation of proxy_mounts with HttpUrl set
    # This would fail with the old code (AttributeError: 'HttpUrl' object has no attribute 'url')
    import app.common.http_client

    importlib.reload(app.common.http_client)

    # Verify clients can be created
    client = app.common.http_client.create_client()
    assert client is not None

    async_client = app.common.http_client.create_async_client()
    assert async_client is not None
