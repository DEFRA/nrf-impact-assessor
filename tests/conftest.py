"""Pytest configuration and shared fixtures.

This file contains fixtures that are shared across multiple test modules.
Test-specific fixtures should be defined in their respective test files.
"""

import pytest

from app.common.auth import require_api_key
from app.main import app


@pytest.fixture(autouse=True)
def _bypass_api_key_auth():
    """Bypass the x-api-key dependency for tests against the main FastAPI app.

    Tests that exercise the auth dependency itself can pop this override or
    construct their own app instance.
    """
    app.dependency_overrides[require_api_key] = lambda: None
    yield
    app.dependency_overrides.pop(require_api_key, None)
