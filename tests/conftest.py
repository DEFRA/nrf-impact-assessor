"""Pytest configuration and shared fixtures.

This file contains fixtures that are shared across multiple test modules.
Test-specific fixtures should be defined in their respective test files.
"""

from datetime import date
from decimal import Decimal

import pytest

from app.calculators.levy import LevyCalculation
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


# The EDP_Name attribute carried by the edp_boundary_extents GeoPackage.
EDP_NAME = (
    "Broads SAC, Broadland Ramsar and River Wensum SAC Environmental "
    "Delivery Plan addressing nutrient pollution (2026 to 2036)"
)


def make_levy_calculation(**overrides) -> LevyCalculation:
    """Build a LevyCalculation carrying every scenario 7 audit field.

    Shared so the audit-field contract lives in one place; pass keyword
    overrides for the fields a test actually cares about.
    """
    fields = {
        "edp_id": 1,
        "edp_name": EDP_NAME,
        "edp_start_date": date(2026, 1, 1),
        "calculation_date": date(2026, 9, 14),
        "calculator_version": 1,
        "units": 10,
        "base_charge_per_unit": Decimal("2193.6649"),
        "rounded_charge_per_unit": Decimal("2193.66"),
        "provisional_amount": Decimal("21936.60"),
        "inflation_adjusted_amount": Decimal("21936.60"),
    }
    return LevyCalculation(**(fields | overrides))
