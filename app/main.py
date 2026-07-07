from contextlib import asynccontextmanager
from logging import getLogger

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.exception_handlers import (
    http_exception_handler as default_http_exception_handler,
)
from fastapi.responses import Response

from app.assess.router import router as assess_router
from app.boundary.router import router as boundary_router
from app.common.auth import require_api_key
from app.common.mongo import get_mongo_client
from app.common.tls import cleanup_cert_files, init_custom_certificates
from app.common.tracing import TraceIdMiddleware
from app.config import ApiServerConfig, DataSyncConfig, DlqAdminConfig, config
from app.data_sync.service import log_startup_table_status
from app.health.router import router as health_router
from app.repositories.engine import warm_shared_engine
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
    try:
        warm_shared_engine()
    except Exception:
        logger.exception("Shared DB engine warmup failed; continuing startup")
    # Surface an empty reference table at boot, independent of any data-sync run.
    log_startup_table_status()
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
# Public (unauthenticated) routers — health and version are used by ELB / k8s probes.
app.include_router(health_router)
app.include_router(version_router)

# All other routers require a valid x-api-key header (service-to-service auth).
protected_dependencies = [Depends(require_api_key)]
app.include_router(assess_router, dependencies=protected_dependencies)
app.include_router(boundary_router, dependencies=protected_dependencies)
app.include_router(tiles_router, dependencies=protected_dependencies)
app.include_router(wwtw_router, dependencies=protected_dependencies)

if ApiServerConfig().testing_enabled:
    from app.test.router import router as test_router

    logger.info("API_TESTING_ENABLED=true: mounting /test/* endpoints")
    app.include_router(
        test_router,
        prefix="/test",
        tags=["test"],
        dependencies=protected_dependencies,
    )

_data_sync_config = DataSyncConfig()
if _data_sync_config.enabled:
    from app.data_sync.router import router as data_sync_router

    # Surface a misconfigured dump location at startup rather than only at
    # reload time; the prefix may legitimately be empty (full bucket-root keys).
    if not _data_sync_config.s3_bucket:
        logger.warning(
            "DATA_SYNC_ENABLED=true but DATA_SYNC_S3_BUCKET is not set; "
            "reloads will fail until it is configured"
        )

    logger.info(
        "DATA_SYNC_ENABLED=true: mounting /admin/data-sync endpoints "
        "(DATA_SYNC_S3_BUCKET=%s, DATA_SYNC_S3_PREFIX=%r)",
        _data_sync_config.s3_bucket,
        _data_sync_config.s3_prefix,
    )
    app.include_router(data_sync_router, tags=["data-sync"])

_dlq_config = DlqAdminConfig()
if _dlq_config.enabled:
    from app.dlq.router import router as dlq_router

    if not _dlq_config.auth_token:
        logger.warning(
            "DLQ_ADMIN_ENABLED=true but DLQ_AUTH_TOKEN is not set; "
            "all /admin/dlq requests will be rejected until it is configured"
        )
    logger.info("DLQ_ADMIN_ENABLED=true: mounting /admin/dlq endpoints")
    app.include_router(dlq_router, tags=["dlq-admin"])


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
