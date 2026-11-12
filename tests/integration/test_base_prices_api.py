"""/base-prices against PostGIS: history, add, change and delete with the
future-price rules enforced inside locked transactions."""

import contextlib
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.main import app
from app.models.db import BasePrice
from app.repositories.repository import Repository
from tests.integration.conftest import insert_base_price

pytestmark = pytest.mark.integration

URL = "/base-prices"
PAST = datetime(2026, 1, 1, tzinfo=UTC)


def _future(days: int = 30) -> datetime:
    return datetime.now(UTC).replace(microsecond=0) + timedelta(days=days)


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _body(**overrides) -> dict:
    return {
        "edpId": 1,
        "basePrice": 3000.5,
        "effectiveFrom": _iso(_future()),
        "comment": "NE advice",
        "staffId": "entra-oid",
        "staffFullName": "Ada Admin",
        "staffEmail": "ada@example.com",
    } | overrides


@pytest.fixture
def client(repository: Repository):
    with patch("app.base_prices.router._get_repository", return_value=repository):
        yield TestClient(app)


def _count(repository: Repository) -> int:
    with repository.session() as session:
        return session.execute(select(func.count()).select_from(BasePrice)).scalar_one()


def test_lists_each_edp_with_its_history_newest_first(
    client: TestClient, repository: Repository
):
    insert_base_price(repository)
    future = _future()
    insert_base_price(repository, price="3000", effective_from=future)

    response = client.get(URL)

    assert response.status_code == 200
    [edp] = response.json()
    assert edp["edpId"] == 1
    assert edp["edpName"] == "Test EDP"
    assert [p["basePrice"] for p in edp["basePrices"]] == [3000.0, 2675.0]
    assert set(edp["basePrices"][0]) == {
        "id",
        "edpId",
        "basePrice",
        "createdAt",
        "effectiveFrom",
        "comment",
        "staffId",
        "staffFullName",
        "staffEmail",
    }


def test_lists_an_edp_with_no_base_prices(client: TestClient, repository: Repository):
    insert_base_price(repository)
    insert_base_price(repository, edp_id=2)
    with repository.session() as session:
        session.execute(BasePrice.__table__.delete().where(BasePrice.edp_id == 2))
        session.commit()

    response = client.get(URL)

    assert [(e["edpId"], len(e["basePrices"])) for e in response.json()] == [
        (1, 1),
        (2, 0),
    ]


def test_adds_a_base_price_and_keeps_the_original(
    client: TestClient, repository: Repository
):
    insert_base_price(repository)

    response = client.post(URL, json=_body())

    assert response.status_code == 201
    created = response.json()
    assert created["basePrice"] == 3000.5
    assert created["staffEmail"] == "ada@example.com"
    assert _count(repository) == 2


def test_add_is_refused_while_a_future_base_price_exists(
    client: TestClient, repository: Repository
):
    insert_base_price(repository)
    insert_base_price(repository, price="3000", effective_from=_future())

    response = client.post(URL, json=_body(effectiveFrom=_iso(_future(60))))

    assert response.status_code == 409
    assert _count(repository) == 2


def test_only_one_of_several_parallel_adds_succeeds(
    client: TestClient, repository: Repository
):
    insert_base_price(repository)
    attempts = 8
    checked_for_future_price = threading.Barrier(attempts, timeout=1)
    real_add = Session.add

    def add_once_every_request_has_checked(session, row):
        # Holds each request between its check and its insert, so without the
        # row lock they all pass the check. With it, only one request gets here.
        with contextlib.suppress(threading.BrokenBarrierError):
            checked_for_future_price.wait()
        return real_add(session, row)

    def post(days: int) -> int:
        body = _body(effectiveFrom=_iso(_future(days)))
        return client.post(URL, json=body).status_code

    with (
        patch.object(Session, "add", add_once_every_request_has_checked),
        ThreadPoolExecutor(max_workers=attempts) as pool,
    ):
        statuses = list(pool.map(post, range(10, 10 + attempts)))

    assert sorted(statuses) == [201] + [409] * (attempts - 1)
    assert _count(repository) == 2


def test_add_is_allowed_once_the_earlier_price_has_taken_effect(
    client: TestClient, repository: Repository
):
    insert_base_price(repository)
    insert_base_price(repository, price="3000", effective_from=PAST + timedelta(days=1))

    response = client.post(URL, json=_body())

    assert response.status_code == 201


def test_add_rejects_an_effective_from_in_the_past(
    client: TestClient, repository: Repository
):
    insert_base_price(repository)

    response = client.post(
        URL, json=_body(effectiveFrom=_iso(datetime.now(UTC) - timedelta(minutes=1)))
    )

    assert response.status_code == 422
    assert _count(repository) == 1


def test_add_for_an_unknown_edp_is_not_found(
    client: TestClient, repository: Repository
):
    insert_base_price(repository)

    response = client.post(URL, json=_body(edpId=99))

    assert response.status_code == 404


def test_changes_a_future_base_price_in_place(
    client: TestClient, repository: Repository
):
    insert_base_price(repository)
    future_id = insert_base_price(repository, price="3000", effective_from=_future())
    new_date = _future(90)

    response = client.patch(
        f"{URL}/{future_id}",
        json={
            "basePrice": 3100,
            "effectiveFrom": _iso(new_date),
            "comment": "Revised",
            "staffId": "other-oid",
            "staffFullName": "Bob Admin",
            "staffEmail": "bob@example.com",
        },
    )

    assert response.status_code == 200
    changed = response.json()
    assert changed["id"] == future_id
    assert changed["basePrice"] == 3100.0
    assert datetime.fromisoformat(changed["effectiveFrom"]) == new_date
    assert changed["comment"] == "Revised"
    assert changed["staffFullName"] == "Bob Admin"
    assert _count(repository) == 2


def test_change_leaves_fields_not_sent_untouched(
    client: TestClient, repository: Repository
):
    future_id = insert_base_price(repository, effective_from=_future())

    response = client.patch(f"{URL}/{future_id}", json={"comment": "Only this"})

    assert response.status_code == 200
    assert response.json()["comment"] == "Only this"
    assert response.json()["basePrice"] == 2675.0
    assert response.json()["staffEmail"] == "test.user@example.com"


def test_change_is_refused_once_the_base_price_has_taken_effect(
    client: TestClient, repository: Repository
):
    past_id = insert_base_price(repository)

    response = client.patch(f"{URL}/{past_id}", json={"basePrice": 1})

    assert response.status_code == 409
    assert client.get(URL).json()[0]["basePrices"][0]["basePrice"] == 2675.0


def test_change_rejects_moving_effective_from_into_the_past(
    client: TestClient, repository: Repository
):
    future_id = insert_base_price(repository, effective_from=_future())

    response = client.patch(
        f"{URL}/{future_id}", json={"effectiveFrom": _iso(PAST + timedelta(days=1))}
    )

    assert response.status_code == 422


def test_change_cannot_move_the_base_price_to_another_edp(
    client: TestClient, repository: Repository
):
    future_id = insert_base_price(repository, effective_from=_future())

    response = client.patch(f"{URL}/{future_id}", json={"edpId": 2})

    assert response.status_code == 422


def test_change_of_an_unknown_base_price_is_not_found(
    client: TestClient, repository: Repository
):
    response = client.patch(f"{URL}/999999", json={"comment": "x"})

    assert response.status_code == 404


def test_deletes_a_future_base_price(client: TestClient, repository: Repository):
    insert_base_price(repository)
    future_id = insert_base_price(repository, price="3000", effective_from=_future())

    response = client.delete(f"{URL}/{future_id}")

    assert response.status_code == 204
    assert _count(repository) == 1


def test_delete_is_refused_once_the_base_price_has_taken_effect(
    client: TestClient, repository: Repository
):
    past_id = insert_base_price(repository)

    response = client.delete(f"{URL}/{past_id}")

    assert response.status_code == 409
    assert _count(repository) == 1


def test_delete_of_an_unknown_base_price_is_not_found(
    client: TestClient, repository: Repository
):
    response = client.delete(f"{URL}/999999")

    assert response.status_code == 404
