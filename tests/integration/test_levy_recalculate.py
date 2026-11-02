"""POST /levy/recalculate against PostGIS: the EDP's charge for the request
date is priced without writing to the database."""

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from app.main import app
from app.models.db import LevyCalculationRecord
from app.repositories.repository import Repository
from tests.integration.conftest import insert_levy_charge

pytestmark = pytest.mark.integration

URL = "/levy/recalculate"
BODY = {"edpId": 1, "units": 4, "requestDate": "2026-10-14"}


@pytest.fixture
def client(repository: Repository):
    with patch("app.levy.router._get_repository", return_value=repository):
        yield TestClient(app)


def test_prices_the_edp_levy_without_an_audit_row(
    client: TestClient, repository: Repository
):
    insert_levy_charge(repository)

    response = client.post(URL, json=BODY)

    assert response.status_code == 200
    assert response.json()["provisionalAmount"] == 10700.0
    assert response.json()["inflationAdjustedAmount"] == 10700.0
    with repository.session() as session:
        assert session.execute(select(LevyCalculationRecord)).first() is None


def test_edp_without_a_charge_is_not_found(client: TestClient, repository: Repository):
    insert_levy_charge(repository)

    response = client.post(URL, json=BODY | {"edpId": 99})

    assert response.status_code == 404


def test_prices_a_base_charge_with_pence(client: TestClient, repository: Repository):
    insert_levy_charge(repository, price="2675.55")

    response = client.post(URL, json=BODY)

    assert response.status_code == 200
    assert response.json()["provisionalAmount"] == 10702.2
