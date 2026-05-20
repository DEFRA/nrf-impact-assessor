import hmac
from logging import getLogger

from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader

from app.config import config

logger = getLogger(__name__)

API_KEY_HEADER = "x-api-key"

_api_key_header = APIKeyHeader(name=API_KEY_HEADER, auto_error=False)


async def require_api_key(provided: str | None = Security(_api_key_header)) -> None:
    """FastAPI dependency that enforces a valid x-api-key header.

    Fails closed: if no key is configured on the server, every request is rejected
    rather than silently accepted.
    """
    expected = config.impact_assessor_api_key
    if not expected:
        logger.error("impact_assessor_api_key is not configured; rejecting request")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Service API key not configured",
        )

    if provided is None or not hmac.compare_digest(provided, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )
