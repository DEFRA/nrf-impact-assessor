from logging import getLogger

import httpx

from app.common.tracing import ctx_trace_id
from app.config import config

logger = getLogger(__name__)

async_proxy_mounts = (
    {
        "http://": httpx.AsyncHTTPTransport(proxy=str(config.http_proxy)),
        "https://": httpx.AsyncHTTPTransport(proxy=str(config.http_proxy)),
    }
    if config.http_proxy
    else {}
)

sync_proxy_mounts = (
    {
        "http://": httpx.HTTPTransport(proxy=str(config.http_proxy)),
        "https://": httpx.HTTPTransport(proxy=str(config.http_proxy)),
    }
    if config.http_proxy
    else {}
)


async def async_hook_request_tracing(request):
    trace_id = ctx_trace_id.get(None)
    if trace_id:
        request.headers[config.tracing_header] = trace_id


def hook_request_tracing(request):
    trace_id = ctx_trace_id.get(None)
    if trace_id:
        request.headers[config.tracing_header] = trace_id


def create_async_client(request_timeout: int = 30) -> httpx.AsyncClient:
    """
    Create an async HTTP client with configurable timeout.

    Args:
        request_timeout: Request timeout in seconds

    Returns:
        Configured httpx.AsyncClient instance
    """
    client_kwargs = {
        "timeout": request_timeout,
        "event_hooks": {"request": [async_hook_request_tracing]},
    }

    if config.http_proxy:
        client_kwargs["mounts"] = async_proxy_mounts

    return httpx.AsyncClient(**client_kwargs)


def create_client(request_timeout: int = 30) -> httpx.Client:
    """
    Create a sync HTTP client with configurable timeout.

    Args:
        request_timeout: Request timeout in seconds

    Returns:
        Configured httpx.Client instance
    """
    client_kwargs = {
        "timeout": request_timeout,
        "event_hooks": {"request": [hook_request_tracing]},
    }

    if config.http_proxy:
        client_kwargs["mounts"] = sync_proxy_mounts

    return httpx.Client(**client_kwargs)
