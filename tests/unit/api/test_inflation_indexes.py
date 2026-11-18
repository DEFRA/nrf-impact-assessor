"""Tests for the /inflation-indexes request validation and API key check."""

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.common.auth import API_KEY_HEADER, require_api_key
from app.config import config
from app.main import app
from tests.unit.api.invalid_request_bodies import (
    INVALID_REQUESTOR_JIRA_TICKET_OVERRIDES,
)

URL = "/inflation-indexes"
ANY_ID = "00000000-0000-0000-0000-000000000000"
VALID_BODY = {
    "chargingYear": datetime.now(UTC).year + 2,
    "cilIndex": 384.21,
    "comment": "NE advice",
    "requestor": "Grace Hopper",
    "jiraTicket": "NRF2-1234",
    "staffId": "entra-oid",
    "staffFullName": "Ada Admin",
    "staffEmail": "ada@example.com",
}
VALID_DELETE_BODY = {
    "comment": "NE advice withdrawn",
    "staffId": "entra-oid",
    "staffFullName": "Ada Admin",
    "staffEmail": "ada@example.com",
}


@pytest.fixture
def client(mocker):
    repository = MagicMock()
    mocker.patch(
        "app.inflation_indexes.router._get_repository", return_value=repository
    )
    mocker.patch(
        "app.inflation_indexes.router.store.add_inflation_index",
        side_effect=AssertionError("must not be reached for an invalid body"),
    )
    return TestClient(app)


@pytest.mark.parametrize(
    "overrides",
    [
        {"cilIndex": 0},
        {"cilIndex": -1},
        {"cilIndex": 10.12345},
        {"cilIndex": 100_000_000},
        {"cilIndex": "abc"},
        {"chargingYear": 2020},
        {"chargingYear": 2101},
        {"comment": "   "},
        {"comment": "x" * 501},
        *INVALID_REQUESTOR_JIRA_TICKET_OVERRIDES,
        {"staffEmail": ""},
        {"staffId": "x" * 37},
        {"extra": 1},
    ],
)
def test_add_rejects_an_invalid_body(client, overrides):
    response = client.post(URL, json=VALID_BODY | overrides)

    assert response.status_code == 422


@pytest.mark.parametrize("missing", list(VALID_BODY))
def test_add_requires_every_field(client, missing):
    body = {k: v for k, v in VALID_BODY.items() if k != missing}

    assert client.post(URL, json=body).status_code == 422


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"cilIndex": None},
        {"chargingYear": 2029},
        {"cilIndex": 1.23456},
        {"requestor": None},
        {"jiraTicket": None},
        {"jiraTicket": "1234"},
    ],
)
def test_change_rejects_an_invalid_body(client, body):
    response = client.patch(f"{URL}/{ANY_ID}", json=body)

    assert response.status_code == 422


@pytest.mark.parametrize(
    "overrides",
    [
        {"comment": "   "},
        {"comment": "x" * 501},
        {"staffEmail": ""},
        {"staffId": "x" * 37},
        {"extra": 1},
    ],
)
def test_delete_rejects_an_invalid_body(client, overrides):
    response = client.request(
        "DELETE", f"{URL}/{ANY_ID}", json=VALID_DELETE_BODY | overrides
    )

    assert response.status_code == 422


@pytest.mark.parametrize("missing", list(VALID_DELETE_BODY))
def test_delete_requires_every_field(client, missing):
    body = {k: v for k, v in VALID_DELETE_BODY.items() if k != missing}

    assert client.request("DELETE", f"{URL}/{ANY_ID}", json=body).status_code == 422


def test_delete_without_a_body_is_rejected(client):
    assert client.delete(f"{URL}/{ANY_ID}").status_code == 422


@pytest.mark.parametrize("api_key", [None, "wrong-key"])
@pytest.mark.parametrize(
    ("method", "url"),
    [
        ("get", URL),
        ("post", URL),
        ("patch", f"{URL}/{ANY_ID}"),
        ("delete", f"{URL}/{ANY_ID}"),
    ],
)
def test_rejects_a_request_without_a_valid_api_key(
    client, mocker, method, url, api_key
):
    app.dependency_overrides.pop(require_api_key, None)
    mocker.patch.object(config, "impact_assessor_api_key", "expected-key")
    headers = {API_KEY_HEADER: api_key} if api_key else {}

    response = getattr(client, method)(url, headers=headers)

    assert response.status_code == 401
