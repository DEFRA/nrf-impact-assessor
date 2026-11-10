"""POST /levy/recalculate against PostGIS: the EDP's base price in force at the
request date-time is priced without writing to the database."""

from datetime import UTC, datetime
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from app.main import app
from app.models.db import LevyCalculationRecord
from app.repositories.repository import Repository
from tests.integration.conftest import insert_base_price

pytestmark = pytest.mark.integration

URL = "/levy/recalculate"
BODY = {"edpId": 1, "units": 4, "requestDate": "2026-10-14T08:30:00Z"}


@pytest.fixture
def client(repository: Repository):
    with patch("app.levy.router._get_repository", return_value=repository):
        yield TestClient(app)


def test_prices_the_edp_levy_without_an_audit_row(
    client: TestClient, repository: Repository
):
    insert_base_price(repository)

    response = client.post(URL, json=BODY)

    assert response.status_code == 200
    assert response.json()["provisionalAmount"] == 10700.0
    assert response.json()["inflationAdjustedAmount"] == 10700.0
    with repository.session() as session:
        assert session.execute(select(LevyCalculationRecord)).first() is None


def test_edp_without_a_base_price_is_not_found(
    client: TestClient, repository: Repository
):
    insert_base_price(repository)

    response = client.post(URL, json=BODY | {"edpId": 99})

    assert response.status_code == 404


def test_prices_a_base_charge_with_pence(client: TestClient, repository: Repository):
    insert_base_price(repository, price="2675.55")

    response = client.post(URL, json=BODY)

    assert response.status_code == 200
    assert response.json()["provisionalAmount"] == 10702.2


def test_prices_the_base_price_in_force_at_the_request_date_time(
    client: TestClient, repository: Repository
):
    insert_base_price(repository, price="2675")
    insert_base_price(
        repository, price="3000", effective_from=datetime(2026, 10, 14, 9, tzinfo=UTC)
    )

    before = client.post(URL, json=BODY | {"requestDate": "2026-10-14T08:59:59Z"})
    at_change = client.post(URL, json=BODY | {"requestDate": "2026-10-14T09:00:00Z"})

    assert before.json()["provisionalAmount"] == 10700.0
    assert at_change.json()["provisionalAmount"] == 12000.0


def test_a_request_before_the_first_base_price_is_not_found(
    client: TestClient, repository: Repository
):
    insert_base_price(repository)

    response = client.post(URL, json=BODY | {"requestDate": "2025-12-31T23:59:59Z"})

    assert response.status_code == 404
