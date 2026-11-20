"""Pytest configuration and shared fixtures.

This file contains fixtures that are shared across multiple test modules.
Test-specific fixtures should be defined in their respective test files.
"""

from datetime import UTC, date, datetime
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
    "Norfolk: Broads Special Area of Conservation (SAC) (Yare and Bure), "
    "Broadland Ramsar and River Wensum SAC Environmental Delivery Plan "
    "addressing nutrient pollution (2027 to 2037)"
)


CALCULATED_AT = datetime(2026, 9, 14, 10, 30, 15, tzinfo=UTC)


def make_levy_calculation(**overrides) -> LevyCalculation:
    """Build a LevyCalculation; pass overrides for the fields a test cares about."""
    fields = {
        "edp_id": 1,
        "edp_start_date": date(2026, 1, 1),
        "calculator_version": 1,
        "units": 10,
        "base_charge_per_unit": Decimal("2675.0000"),
        "base_price_effective_from": datetime(2026, 1, 1, tzinfo=UTC),
        "original_base_charge_per_unit": Decimal("2675.0000"),
        "provisional_amount": Decimal("26750.00"),
        "original_base_amount": Decimal("26750.00"),
        "inflation_adjusted_amount": Decimal("26750.00"),
    }
    return LevyCalculation(**(fields | overrides))
