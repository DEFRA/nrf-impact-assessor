"""Tests for the /base-prices request validation and API key check."""

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.common.auth import API_KEY_HEADER, require_api_key
from app.config import config
from app.main import app

URL = "/base-prices"
VALID_BODY = {
    "edpId": 1,
    "basePrice": 2700.5,
    "effectiveFrom": "2099-01-01T00:00:00Z",
    "comment": "NE advice",
    "staffId": "entra-oid",
    "staffFullName": "Ada Admin",
    "staffEmail": "ada@example.com",
}


@pytest.fixture
def client(mocker):
    repository = MagicMock()
    mocker.patch("app.base_prices.router._get_repository", return_value=repository)
    mocker.patch(
        "app.base_prices.router.store.add_base_price",
        side_effect=AssertionError("must not be reached for an invalid body"),
    )
    return TestClient(app)


@pytest.mark.parametrize(
    "overrides",
    [
        {"basePrice": 0},
        {"basePrice": -1},
        {"basePrice": 10.123},
        {"basePrice": 100_000_000},
        {"basePrice": "abc"},
        {"effectiveFrom": "2099-01-01"},
        {"effectiveFrom": "2099-01-01T00:00:00"},
        {"effectiveFrom": "2099-01-01T01:00:00+01:00"},
        {"comment": "   "},
        {"comment": "x" * 501},
        {"staffEmail": ""},
        {"staffId": "x" * 37},
        {"edpId": 0},
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
    [{}, {"basePrice": None}, {"edpId": 2}, {"basePrice": 1.234}],
)
def test_change_rejects_an_invalid_body(client, body):
    assert client.patch(f"{URL}/1", json=body).status_code == 422


@pytest.mark.parametrize("api_key", [None, "wrong-key"])
@pytest.mark.parametrize(
    ("method", "url"),
    [
        ("get", URL),
        ("post", URL),
        ("patch", f"{URL}/1"),
        ("delete", f"{URL}/1"),
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
