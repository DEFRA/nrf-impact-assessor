"""Tests for the POST /levy/recalculate endpoint."""

from datetime import UTC, date, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from app.common.auth import API_KEY_HEADER, require_api_key
from app.config import config
from app.main import app
from app.repositories.levy import LevyPricingRows
from tests.conftest import EDP_NAME

URL = "/levy/recalculate"
VALID_BODY = {"edpId": 1, "units": 10, "requestDate": "2026-10-14"}


def _index(factor: str) -> SimpleNamespace:
    return SimpleNamespace(id=uuid4(), index_factor=Decimal(factor))


def _charge(edp_start_date: date = date(2026, 1, 1)) -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid4(),
        edp_id=1,
        edp_name=EDP_NAME,
        edp_start_date=edp_start_date,
        base_charge_per_unit=Decimal("2675.0000"),
    )


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def session(mocker):
    session = MagicMock()
    repository = MagicMock()
    repository.session.return_value.__enter__.return_value = session
    mocker.patch("app.levy.router.get_shared_repository", return_value=repository)
    return session


@pytest.fixture
def db(mocker, session):
    return {
        "pricing": mocker.patch(
            "app.levy.service.get_levy_pricing_rows",
            return_value=LevyPricingRows(_charge(), None, None),
        ),
    }


def test_returns_provisional_and_inflation_adjusted_levy(client, db):
    response = client.post(URL, json=VALID_BODY)

    assert response.status_code == 200
    assert response.json() | {"calculationDate": None} == {
        "calculationDate": None,
        "provisionalAmount": 26750.0,
        "inflationAdjustedAmount": 26750.0,
    }


def test_returns_the_calculation_time_as_utc_iso_datetime(client, db):
    before = datetime.now(UTC)
    response = client.post(URL, json=VALID_BODY)
    after = datetime.now(UTC)

    calculation_date = response.json()["calculationDate"]
    assert calculation_date.endswith("Z")
    assert before <= datetime.fromisoformat(calculation_date) <= after


def test_prices_the_edp_on_the_requested_date(client, db):
    client.post(URL, json=VALID_BODY)

    assert db["pricing"].call_args.args[1:] == (1, date(2026, 10, 14))


@pytest.mark.parametrize(
    "body",
    [
        {"units": 10, "requestDate": "2026-10-14"},
        VALID_BODY | {"edpId": None},
        VALID_BODY | {"edpId": ""},
    ],
)
def test_prices_edp_1_when_the_edp_is_empty(client, db, body):
    response = client.post(URL, json=body)

    assert response.status_code == 200
    assert db["pricing"].call_args.args[1] == 1


def test_does_not_write_to_the_database(client, db, session):
    client.post(URL, json=VALID_BODY)

    session.add.assert_not_called()
    session.commit.assert_not_called()


def test_does_not_inflate_the_levy_in_a_later_charging_year(client, db):
    db["pricing"].return_value = LevyPricingRows(
        _charge(), _index("1.0000"), _index("1.0500")
    )

    response = client.post(URL, json=VALID_BODY | {"requestDate": "2027-03-01"})

    assert response.status_code == 200
    assert response.json()["provisionalAmount"] == 26750.0
    assert response.json()["inflationAdjustedAmount"] == 26750.0


def test_returns_not_found_when_no_charge_exists_for_the_edp(client, db):
    db["pricing"].return_value = None

    response = client.post(URL, json=VALID_BODY)

    assert response.status_code == 404


def test_returns_unprocessable_when_the_levy_is_unavailable(client, db):
    charge = _charge()
    charge.base_charge_per_unit = Decimal("2675.50")
    db["pricing"].return_value = LevyPricingRows(charge, None, None)

    response = client.post(URL, json=VALID_BODY)

    assert response.status_code == 422


@pytest.mark.parametrize(
    "rows",
    [
        LevyPricingRows(_charge(), None, None),
        LevyPricingRows(_charge(), None, _index("1.0500")),
        LevyPricingRows(_charge(), _index("1.0000"), None),
    ],
)
def test_prices_a_later_charging_year_without_inflation_indices(client, db, rows):
    db["pricing"].return_value = rows

    response = client.post(URL, json=VALID_BODY | {"requestDate": "2027-06-15"})

    assert response.status_code == 200
    assert response.json()["provisionalAmount"] == 26750.0
    assert response.json()["inflationAdjustedAmount"] == 26750.0


@pytest.mark.parametrize(
    "body",
    [
        {},
        VALID_BODY | {"edpId": 0},
        VALID_BODY | {"edpId": "one"},
        VALID_BODY | {"edpId": True},
        VALID_BODY | {"units": 0},
        VALID_BODY | {"units": -1},
        VALID_BODY | {"units": "ten"},
        VALID_BODY | {"units": True},
        VALID_BODY | {"units": "10"},
        VALID_BODY | {"units": 10.0},
        VALID_BODY | {"edpId": "1"},
        VALID_BODY | {"requestDate": 1791936000},
        {"edpId": 1, "requestDate": "2026-10-14"},
        VALID_BODY | {"requestDate": "14/10/2026"},
        {"edpId": 1, "units": 10},
        VALID_BODY | {"unexpected": True},
        VALID_BODY | {"nrfReference": "NRL-000001"},
        {"edp_id": 1, "units": 10, "request_date": "2026-10-14"},
        [VALID_BODY],
    ],
)
def test_returns_bad_request_for_an_invalid_body(client, db, body):
    response = client.post(URL, json=body)

    assert response.status_code == 400
    db["pricing"].assert_not_called()


@pytest.mark.parametrize(
    "content",
    [
        b"{not json",
        b'{"units": "\xff"}',
        b'{"units": "\\ud800", "requestDate": "2026-10-14"}',
        b'{"units": 10, "requestDate": "\\ud800"}',
        b'{"units": 10, "requestDate": "2026-10-14", "\\ud800": 1}',
        b"",
    ],
)
def test_returns_bad_request_for_malformed_json(client, db, content):
    response = client.post(
        URL, content=content, headers={"content-type": "application/json"}
    )

    assert response.status_code == 400
    db["pricing"].assert_not_called()


@pytest.mark.parametrize("api_key", [None, "wrong-key"])
def test_rejects_a_request_without_a_valid_api_key(client, db, mocker, api_key):
    app.dependency_overrides.pop(require_api_key, None)
    mocker.patch.object(config, "impact_assessor_api_key", "expected-key")
    headers = {API_KEY_HEADER: api_key} if api_key else {}

    response = client.post(URL, json=VALID_BODY, headers=headers)

    assert response.status_code == 401
    db["pricing"].assert_not_called()
