"""POST /levy/recalculate against PostGIS: the EDP's charge for the request
date is priced without writing to the database."""

from datetime import date
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, text

from app.main import app
from app.models.db import LevyCalculationRecord
from app.repositories.repository import Repository

pytestmark = pytest.mark.integration

URL = "/levy/recalculate"


@pytest.fixture
def client(repository: Repository):
    with patch("app.levy.router.get_shared_repository", return_value=repository):
        yield TestClient(app)


@pytest.fixture
def levy_charge(repository: Repository) -> None:
    with repository.session() as session:
        session.execute(
            text(
                "INSERT INTO public.levy_charges "
                "(id, edp_id, edp_name, edp_start_date, charge_valid_from, "
                "charge_valid_to, base_charge_per_unit) VALUES "
                "(gen_random_uuid(), 1, 'Test EDP', :f, :f, :t, 2675)"
            ),
            {"f": date(2026, 1, 1), "t": date(2026, 12, 31)},
        )
        session.commit()


def test_prices_the_edp_levy_without_an_audit_row(
    client: TestClient, repository: Repository, levy_charge: None
):
    response = client.post(
        URL, json={"edpId": 1, "units": 4, "requestDate": "2026-10-14"}
    )

    assert response.status_code == 200
    assert response.json()["provisionalAmount"] == 10700.0
    assert response.json()["inflationAdjustedAmount"] == 10700.0
    with repository.session() as session:
        assert session.execute(select(LevyCalculationRecord)).first() is None


def test_edp_without_a_charge_is_not_found(client: TestClient, levy_charge: None):
    response = client.post(
        URL, json={"edpId": 99, "units": 4, "requestDate": "2026-10-14"}
    )

    assert response.status_code == 404
