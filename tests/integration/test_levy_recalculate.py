"""POST /levy/recalculate against PostGIS: the EDP's base price in force at the
request date-time is priced, with inflation, without writing to the database."""

from datetime import UTC, datetime
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.repositories.repository import Repository
from tests.integration.conftest import insert_base_price, insert_inflation_index

pytestmark = pytest.mark.integration

URL = "/levy/recalculate"
BODY = {"edpId": 1, "units": 4, "requestDate": "2026-10-14T08:30:00Z"}


@pytest.fixture
def client(repository: Repository):
    insert_inflation_index(repository, 2026, cil_index="400")
    with patch("app.levy.router._get_repository", return_value=repository):
        yield TestClient(app)


def test_prices_the_edp_levy(client: TestClient, repository: Repository):
    insert_base_price(repository)

    response = client.post(URL, json=BODY)

    assert response.status_code == 200
    assert response.json()["provisionalAmount"] == 10700.0
    assert response.json()["inflationAdjustedAmount"] == 10700.0


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
    """An existing quote picks up a changed base price from its effective_from
    and not before. A past request date, such as a commitment certificate's,
    keeps the price in force then whatever has been added since."""
    insert_base_price(repository, price="2675")
    insert_base_price(
        repository, price="3000", effective_from=datetime(2026, 10, 14, 9, tzinfo=UTC)
    )

    before = client.post(URL, json=BODY | {"requestDate": "2026-10-14T08:59:59Z"})
    at_change = client.post(URL, json=BODY | {"requestDate": "2026-10-14T09:00:00Z"})

    assert before.json()["provisionalAmount"] == 10700.0
    assert at_change.json()["provisionalAmount"] == 12000.0


def test_inflates_example_a_in_2030(client: TestClient, repository: Repository):
    insert_base_price(repository, effective_from=datetime(2027, 1, 1, tzinfo=UTC))
    for year, value in ((2027, "412"), (2028, "417"), (2029, "422"), (2030, "427")):
        insert_inflation_index(repository, year, cil_index=value)

    response = client.post(
        URL,
        json={"edpId": 1, "units": 1000, "requestDate": "2030-06-24T12:00:00Z"},
    )

    assert response.status_code == 200
    assert response.json()["provisionalAmount"] == 2675000.0
    assert response.json()["inflationAdjustedAmount"] == 2772390.78


@pytest.mark.parametrize(
    ("request_date", "adjusted"),
    [("2027-12-31T23:59:59Z", 2675000.0), ("2028-01-01T00:00:00Z", 2707463.59)],
)
def test_the_index_year_changes_at_midnight_utc(
    client: TestClient, repository: Repository, request_date: str, adjusted: float
):
    insert_base_price(repository, effective_from=datetime(2027, 1, 1, tzinfo=UTC))
    insert_inflation_index(repository, 2027, cil_index="412")
    insert_inflation_index(repository, 2028, cil_index="417")

    response = client.post(
        URL, json={"edpId": 1, "units": 1000, "requestDate": request_date}
    )

    assert response.json()["inflationAdjustedAmount"] == adjusted


def test_a_changed_base_price_restarts_inflation_from_its_year(
    client: TestClient, repository: Repository
):
    insert_base_price(repository, price="2675")
    insert_base_price(
        repository, price="3000", effective_from=datetime(2027, 2, 1, tzinfo=UTC)
    )
    insert_inflation_index(repository, 2027, cil_index="420")

    response = client.post(URL, json=BODY | {"requestDate": "2027-03-01T12:00:00Z"})

    assert response.status_code == 200
    assert response.json()["provisionalAmount"] == 12000.0
    assert response.json()["inflationAdjustedAmount"] == 12000.0
