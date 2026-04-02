"""HTTP client for nrf-backend API callbacks."""

import logging
import time

import httpx

from app.common.http_client import create_client

logger = logging.getLogger(__name__)


class BackendClient:
    """Sends assessment results back to nrf-backend via PATCH /quotes/{reference}."""

    def __init__(self, base_url: str, timeout: int = 30, max_retries: int = 3):
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self._client = create_client(request_timeout=timeout)

    def patch_quote(self, reference: str, payload: dict) -> None:
        """PATCH /quotes/{reference} with assessment results.

        Retries on 5xx and transport errors with exponential backoff.
        Does not retry on 400 (bad payload) or 404 (quote not found).

        Args:
            reference: Quote reference (e.g. "NRF-000001")
            payload: Request body matching the PATCH schema

        Raises:
            httpx.HTTPStatusError: On non-retryable HTTP errors (400, 404)
                after logging, or on retryable errors after max retries.
            httpx.TransportError: On transport errors after max retries.
        """
        url = f"{self.base_url}/quotes/{reference}"

        for attempt in range(self.max_retries + 1):
            try:
                response = self._client.patch(url, json=payload)
                response.raise_for_status()
                logger.info(f"PATCH {url} succeeded (HTTP {response.status_code})")
                return
            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                if status in (400, 404):
                    logger.error(
                        f"PATCH {url} failed with HTTP {status} (not retryable): "
                        f"{e.response.text}"
                    )
                    raise
                if attempt < self.max_retries:
                    wait = 2**attempt
                    logger.warning(
                        f"PATCH {url} failed with HTTP {status}, "
                        f"retrying in {wait}s (attempt {attempt + 1}/{self.max_retries})"
                    )
                    time.sleep(wait)
                else:
                    logger.error(
                        f"PATCH {url} failed with HTTP {status} after "
                        f"{self.max_retries} retries"
                    )
                    raise
            except httpx.TransportError as e:
                if attempt < self.max_retries:
                    wait = 2**attempt
                    logger.warning(
                        f"PATCH {url} transport error: {e}, "
                        f"retrying in {wait}s (attempt {attempt + 1}/{self.max_retries})"
                    )
                    time.sleep(wait)
                else:
                    logger.error(
                        f"PATCH {url} transport error after {self.max_retries} retries: {e}"
                    )
                    raise
