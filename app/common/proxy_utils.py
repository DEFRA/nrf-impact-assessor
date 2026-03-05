"""Proxy configuration utilities for CDP environment."""

import logging
import os

logger = logging.getLogger(__name__)


def configure_proxy_settings() -> None:
    """Configure and log proxy-related environment variables.

    If HTTP_PROXY is set but HTTPS_PROXY is not, copies the HTTP_PROXY value
    to HTTPS_PROXY to ensure HTTPS requests also use the proxy. This is needed
    in environments like CDP where only HTTP_PROXY may be configured.
    """
    # Ensure HTTPS_PROXY is set if HTTP_PROXY is defined
    http_proxy = os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy")
    https_proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")

    if http_proxy and not https_proxy:
        os.environ["HTTPS_PROXY"] = http_proxy
        logger.info(f"HTTPS_PROXY not set, copying from HTTP_PROXY: {http_proxy}")

    # Log all proxy settings
    proxy_vars = [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "http_proxy",
        "https_proxy",
        "NO_PROXY",
        "no_proxy",
        "ALL_PROXY",
        "all_proxy",
    ]

    found_any = False
    for var in proxy_vars:
        value = os.environ.get(var)
        if value:
            found_any = True
            # Mask credentials if present in proxy URL (user:pass@host)
            if "@" in value:
                masked = value.split("@")[-1]
                logger.info(f"Proxy env var {var}=***@{masked}")
            else:
                logger.info(f"Proxy env var {var}={value}")

    if not found_any:
        logger.info("No proxy environment variables detected")
