from contextlib import asynccontextmanager
from logging import getLogger

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.exception_handlers import (
    http_exception_handler as default_http_exception_handler,
)
from fastapi.responses import Response

from app.assess.router import router as assess_router
from app.boundary.router import router as boundary_router
from app.common.mongo import get_mongo_client
from app.common.tls import cleanup_cert_files, init_custom_certificates
from app.common.tracing import TraceIdMiddleware
from app.config import ApiServerConfig, config
from app.health.router import router as health_router
from app.tiles.router import router as tiles_router
from app.version.router import router as version_router
from app.wwtw.router import router as wwtw_router

logger = getLogger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Startup
    init_custom_certificates()
    client = await get_mongo_client()
    logger.info("MongoDB client connected")
    yield
    # Shutdown
    if client:
        await client.close()
        logger.info("MongoDB client closed")
    cleanup_cert_files()


app = FastAPI(title="NRF Impact Assessor API", lifespan=lifespan)


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException) -> Response:
    request.state.error_detail = exc.detail
    return await default_http_exception_handler(request, exc)


# Setup middleware
app.add_middleware(TraceIdMiddleware)

# Setup Routes
app.include_router(health_router)
app.include_router(version_router)
app.include_router(assess_router)
app.include_router(boundary_router)
app.include_router(tiles_router)
app.include_router(wwtw_router)

if ApiServerConfig().testing_enabled:
    from app.test.router import router as test_router

    logger.info("API_TESTING_ENABLED=true: mounting /test/* endpoints")
    app.include_router(test_router, prefix="/test", tags=["test"])


def main() -> None:  # pragma: no cover
    uvicorn.run(
        "app.main:app",
        host=config.host,
        port=config.port,
        workers=config.workers,
        log_config=config.log_config,
    )


if __name__ == "__main__":  # pragma: no cover
    main()
