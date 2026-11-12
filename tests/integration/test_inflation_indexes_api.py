"""/inflation-indexes against PostGIS: list, add, change and delete with the
one-row-per-year and future-year rules enforced, and the factor derived from
the 2026 base row."""

import contextlib
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import patch
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.main import app
from app.models.db import LevyInflationIndex
from app.repositories.levy import record_levy_calculation
from app.repositories.repository import Repository
from tests.conftest import make_levy_calculation
from tests.integration.conftest import insert_base_price, insert_inflation_index

pytestmark = pytest.mark.integration

URL = "/inflation-indexes"
BASE_YEAR = 2026


def _future_year() -> int:
    return datetime.now(UTC).year + 2


def _current_year() -> int:
    return datetime.now(UTC).year


def _body(**overrides) -> dict:
    return {
        "chargingYear": _future_year(),
        "cilIndex": 384.21,
        "comment": "NE advice",
        "staffId": "entra-oid",
        "staffFullName": "Ada Admin",
        "staffEmail": "ada@example.com",
    } | overrides


@pytest.fixture
def client(repository: Repository):
    with patch("app.inflation_indexes.router._get_repository", return_value=repository):
        yield TestClient(app)


def _count(repository: Repository) -> int:
    with repository.session() as session:
        return session.execute(
            select(func.count()).select_from(LevyInflationIndex)
        ).scalar_one()


def test_lists_the_history_newest_year_first_with_a_derived_effective_from(
    client: TestClient, repository: Repository
):
    insert_inflation_index(repository, BASE_YEAR)
    insert_inflation_index(repository, _future_year(), cil_index="384.21")

    response = client.get(URL)

    assert response.status_code == 200
    [newest, base] = response.json()
    assert [row["chargingYear"] for row in (newest, base)] == [
        _future_year(),
        BASE_YEAR,
    ]
    assert set(newest) == {
        "id",
        "chargingYear",
        "cilIndex",
        "indexFactor",
        "effectiveFrom",
        "createdAt",
        "comment",
        "staffId",
        "staffFullName",
        "staffEmail",
    }
    assert datetime.fromisoformat(newest["effectiveFrom"]) == datetime(
        _future_year(), 1, 1, tzinfo=UTC
    )


def test_adds_an_index_with_the_factor_derived_from_the_base_row(
    client: TestClient, repository: Repository
):
    insert_inflation_index(repository, BASE_YEAR)

    response = client.post(URL, json=_body())

    assert response.status_code == 201
    created = response.json()
    assert created["chargingYear"] == _future_year()
    assert created["cilIndex"] == 384.21
    assert created["indexFactor"] == 0.9605  # 384.21 / 400, 4 dp half-up
    assert created["staffEmail"] == "ada@example.com"
    assert _count(repository) == 2


def test_add_keeps_the_existing_years_untouched(
    client: TestClient, repository: Repository
):
    insert_inflation_index(repository, BASE_YEAR)
    with repository.session() as session:
        base_row_id = session.execute(
            select(LevyInflationIndex.id).where(
                LevyInflationIndex.charging_year == BASE_YEAR
            )
        ).scalar_one()

    response = client.post(URL, json=_body())

    assert response.status_code == 201
    with repository.session() as session:
        base_row = session.scalars(
            select(LevyInflationIndex).where(
                LevyInflationIndex.charging_year == BASE_YEAR
            )
        ).one()
    assert (base_row.id, base_row.cil_index, base_row.index_factor) == (
        base_row_id,
        Decimal("400.0000"),
        Decimal("1.0000"),
    )


def test_add_for_a_year_that_already_has_an_index_conflicts(
    client: TestClient, repository: Repository
):
    insert_inflation_index(repository, BASE_YEAR)
    taken_year = _future_year()
    insert_inflation_index(repository, taken_year)

    response = client.post(URL, json=_body())

    assert response.status_code == 409
    assert _count(repository) == 2


def test_only_one_of_several_parallel_adds_for_a_year_succeeds(
    client: TestClient, repository: Repository
):
    insert_inflation_index(repository, BASE_YEAR)
    attempts = 8
    checked_for_duplicate = threading.Barrier(attempts, timeout=1)
    real_add = Session.add

    def add_once_every_request_has_checked(session, row):
        # Holds each request between its duplicate check and its insert, so
        # without the unique constraint they all pass the check.
        with contextlib.suppress(threading.BrokenBarrierError):
            checked_for_duplicate.wait()
        return real_add(session, row)

    def post() -> int:
        return client.post(URL, json=_body()).status_code

    with (
        patch.object(Session, "add", add_once_every_request_has_checked),
        ThreadPoolExecutor(max_workers=attempts) as pool,
    ):
        statuses = list(pool.map(lambda _: post(), range(attempts)))

    assert sorted(statuses) == [201] + [409] * (attempts - 1)
    assert _count(repository) == 2


def test_add_for_the_current_charging_year_is_rejected(
    client: TestClient, repository: Repository
):
    insert_inflation_index(repository, BASE_YEAR)

    response = client.post(URL, json=_body(chargingYear=_current_year()))

    assert response.status_code == 422
    assert _count(repository) == 1


def test_add_without_the_base_row_cannot_derive_a_factor(
    client: TestClient, repository: Repository
):
    response = client.post(URL, json=_body())

    assert response.status_code == 500
    assert "base charging year" in response.json()["detail"]
    assert _count(repository) == 0


def test_changes_a_future_index_rederiving_the_factor(
    client: TestClient, repository: Repository
):
    insert_inflation_index(repository, BASE_YEAR)
    future_id = insert_inflation_index(repository, _future_year())

    response = client.patch(
        f"{URL}/{future_id}",
        json={
            "cilIndex": 420.00,
            "comment": "Revised",
            "staffId": "other-oid",
            "staffFullName": "Bob Admin",
            "staffEmail": "bob@example.com",
        },
    )

    assert response.status_code == 200
    changed = response.json()
    assert UUID(changed["id"]) == future_id
    assert changed["cilIndex"] == 420.00
    assert changed["indexFactor"] == 1.05  # 420 / 400
    assert changed["comment"] == "Revised"
    assert changed["staffFullName"] == "Bob Admin"
    assert datetime.fromisoformat(changed["effectiveFrom"]) == datetime(
        _future_year(), 1, 1, tzinfo=UTC
    )


def test_change_leaves_fields_not_sent_untouched(
    client: TestClient, repository: Repository
):
    insert_inflation_index(repository, BASE_YEAR)
    future_id = insert_inflation_index(repository, _future_year())

    response = client.patch(f"{URL}/{future_id}", json={"comment": "Only this"})

    assert response.status_code == 200
    assert response.json()["comment"] == "Only this"
    assert response.json()["cilIndex"] == 400.0
    assert response.json()["indexFactor"] == 1.0
    assert response.json()["staffEmail"] == "test.user@example.com"


def test_change_cannot_move_an_index_to_another_year(
    client: TestClient, repository: Repository
):
    insert_inflation_index(repository, BASE_YEAR)
    future_id = insert_inflation_index(repository, _future_year())

    response = client.patch(f"{URL}/{future_id}", json={"chargingYear": 2099})

    assert response.status_code == 422


def test_change_is_refused_once_the_charging_year_has_begun(
    client: TestClient, repository: Repository
):
    # The current year's row is the live index: its year has begun.
    current_id = insert_inflation_index(repository, _current_year())

    response = client.patch(f"{URL}/{current_id}", json={"cilIndex": 1})

    assert response.status_code == 409


def test_change_of_an_unknown_index_is_not_found(client: TestClient):
    response = client.patch(
        f"{URL}/00000000-0000-0000-0000-000000000000", json={"comment": "x"}
    )

    assert response.status_code == 404


def test_deletes_a_future_index(client: TestClient, repository: Repository):
    insert_inflation_index(repository, BASE_YEAR)
    future_id = insert_inflation_index(repository, _future_year())

    response = client.delete(f"{URL}/{future_id}")

    assert response.status_code == 204
    assert _count(repository) == 1


def test_delete_is_refused_once_the_charging_year_has_begun(
    client: TestClient, repository: Repository
):
    current_id = insert_inflation_index(repository, _current_year())

    response = client.delete(f"{URL}/{current_id}")

    assert response.status_code == 409
    assert _count(repository) == 1


def test_delete_of_an_index_that_priced_a_quote_is_refused(
    client: TestClient, repository: Repository
):
    charge_id = insert_base_price(repository)
    start_id = insert_inflation_index(repository, BASE_YEAR)
    future_id = insert_inflation_index(repository, _future_year())
    levy = make_levy_calculation(
        levy_charge_id=charge_id,
        edp_start_year_index_id=start_id,
        edp_start_year_index_factor=Decimal("1.0000"),
        calculation_year_index_id=future_id,
        calculation_year_index_factor=Decimal("1.0000"),
    )
    with repository.session() as session:
        record_levy_calculation(session, "NRL-000001", levy)
        session.commit()

    response = client.delete(f"{URL}/{future_id}")

    assert response.status_code == 409
    assert "used to price a quote" in response.json()["detail"]
    assert _count(repository) == 2


def test_delete_of_an_unknown_index_is_not_found(client: TestClient):
    response = client.delete(f"{URL}/00000000-0000-0000-0000-000000000000")

    assert response.status_code == 404
