import contextvars
from logging import getLogger

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

from app.config import config

logger = getLogger(__name__)

ctx_trace_id = contextvars.ContextVar("trace_id")
ctx_request = contextvars.ContextVar("request")
ctx_response = contextvars.ContextVar("response")


# Inbound HTTP requests on the platform will have a `x-cdp-request-id` header.
# This can be used to follow a single request across multiple services.
# TraceIdMiddleware handles extracting the tracing header and persisting it
# for the duration of the request in the ContextVar `ctx_trace_id`.
class TraceIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        req_trace_id = request.headers.get(config.tracing_header, None)
        if req_trace_id:
            ctx_trace_id.set(req_trace_id)

        ctx_request.set({"url": str(request.url), "method": request.method})

        response = await call_next(request)
        ctx_response.set({"status_code": response.status_code})

        if response.status_code >= 400:
            level = "warning" if response.status_code < 500 else "error"
            detail = getattr(request.state, "error_detail", None)
            msg = "HTTP %d: %s %s" + (" - %s" if detail else "")
            args = (response.status_code, request.method, request.url.path)
            if detail:
                args = (*args, detail)
            getattr(logger, level)(msg, *args)

        return response
