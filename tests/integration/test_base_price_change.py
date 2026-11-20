"""A base price change against PostGIS, end to end: a new quote is priced at the base price in force when it is calculated, and a price staff
add through /base-prices is the one /levy/recalculate uses from its
effective_from."""

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.models.job import BoundaryGeojson, ImpactAssessmentJob, IntersectingEdp
from app.orchestrator import JobOrchestrator
from app.repositories.repository import Repository
from tests.integration.conftest import (
    insert_base_price,
    insert_edp_boundary,
    insert_inflation_index,
)

pytestmark = pytest.mark.integration

EDP_ID = 1
EDP_LABEL = "Test EDP"
UNITS = 4
WKT = (
    "MULTIPOLYGON(((600000 300000, 601000 300000, 601000 301000, "
    "600000 301000, 600000 300000)))"
)


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _flat_indices(repository: Repository, first: int, last: int) -> None:
    """The same index every year, so nothing is inflated."""
    for year in range(first, last + 1):
        insert_inflation_index(repository, year)


def _job() -> ImpactAssessmentJob:
    return ImpactAssessmentJob(
        reference="NRL-000001",
        boundary_geojson=BoundaryGeojson(
            boundary_geometry_original={"type": "Polygon", "coordinates": []},
            intersecting_edps=[IntersectingEdp(label=EDP_LABEL)],
        ),
        housingUnits=UNITS,
    )


def test_a_new_quote_is_priced_at_the_base_price_in_force_now(
    repository: Repository,
):
    now = datetime.now(UTC)
    edp_start = date(now.year, 1, 1)
    _flat_indices(repository, now.year - 1, now.year)
    insert_edp_boundary(repository, EDP_LABEL, WKT, edp_id=EDP_ID)
    insert_base_price(
        repository,
        effective_from=now - timedelta(days=400),
        price="2675",
        edp_start_date=edp_start,
    )
    insert_base_price(repository, effective_from=now - timedelta(days=1), price="3000")
    insert_base_price(repository, effective_from=now + timedelta(days=30), price="3500")

    priced = JobOrchestrator(MagicMock(), repository)._calculate_levy(_job())

    assert priced.levy.original_base_charge_per_unit == Decimal("2675")
    assert priced.levy.original_base_amount == Decimal("10700.00")
    assert priced.levy.base_charge_per_unit == Decimal("3000")
    assert priced.levy.provisional_amount == Decimal("12000.00")
    assert priced.levy.inflation_adjusted_amount == Decimal("12000.00")


@pytest.fixture
def client(repository: Repository):
    with (
        patch("app.base_prices.router._get_repository", return_value=repository),
        patch("app.levy.router._get_repository", return_value=repository),
    ):
        yield TestClient(app)


def test_a_base_price_added_by_staff_is_priced_from_its_effective_from(
    client: TestClient, repository: Repository
):
    insert_base_price(repository, price="2675")
    effective_from = datetime.now(UTC).replace(microsecond=0) + timedelta(days=30)
    _flat_indices(repository, 2026, effective_from.year)

    added = client.post(
        "/base-prices",
        json={
            "edpId": EDP_ID,
            "basePrice": 3000,
            "effectiveFrom": _iso(effective_from),
            "comment": "NE advice",
            "requestor": "Grace Hopper",
            "jiraTicket": "NRF2-1234",
            "staffId": "entra-oid",
            "staffFullName": "Ada Admin",
            "staffEmail": "ada@example.com",
        },
    )
    assert added.status_code == 201

    def recalculate(request_date: datetime):
        return client.post(
            "/levy/recalculate",
            json={
                "edpId": EDP_ID,
                "units": UNITS,
                "requestDate": _iso(request_date),
            },
        )

    before = recalculate(effective_from - timedelta(seconds=1))
    at_change = recalculate(effective_from)

    assert before.status_code == 200
    assert before.json()["provisionalAmount"] == 10700.0
    assert at_change.status_code == 200
    assert at_change.json()["provisionalAmount"] == 12000.0
