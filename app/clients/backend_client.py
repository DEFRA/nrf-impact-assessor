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
        logger.info(f"Attempting PATCH {url} to backend")
        start = time.monotonic()
        last_exception = None

        for attempt in range(self.max_retries + 1):
            try:
                response = self._client.patch(url, json=payload)
                response.raise_for_status()
                elapsed = time.monotonic() - start
                logger.info(
                    f"PATCH {url} succeeded (HTTP {response.status_code}) "
                    f"in {elapsed:.2f}s"
                )
                return
            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                if status in (400, 404):
                    logger.error(
                        f"PATCH {url} failed with HTTP {status} (not retryable): "
                        f"{e.response.text}"
                    )
                    raise
                logger.warning(
                    f"PATCH {url} returned HTTP {status}: {e.response.text[:500]}"
                )
                last_exception = e
            except httpx.TransportError as e:
                last_exception = e

            if attempt < self.max_retries:
                wait = 2**attempt
                logger.warning(
                    f"PATCH {url} failed ({last_exception}), "
                    f"retrying in {wait}s (attempt {attempt + 1}/{self.max_retries})"
                )
                time.sleep(wait)

        elapsed = time.monotonic() - start
        logger.error(
            f"PATCH {url} failed after {self.max_retries} retries in {elapsed:.2f}s"
        )
        raise last_exception
