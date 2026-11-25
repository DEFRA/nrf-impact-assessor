"""The levy against the migration seeds alone: the 2026 base price with
indices 2026 = 400 and 2027 = 400 prices 2026 and 2027 with no inflation. The
go-live steps (a base price effective 1 January 2027, then the 2027 index
updated), made through the staff APIs before 1 January 2027, keep 2027
uninflated; from 1 January 2027 both APIs refuse them."""

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.levy.service import price_levy
from app.main import app
from app.repositories.repository import Repository
from tests.integration.conftest import run_alembic, scratch_database

pytestmark = pytest.mark.integration

MIGRATION_DB = "test_nrf_impact_levy_seeded"
# The go-live steps commit through the APIs, so they get their own database
# and never reach the seeded-year tests.
GO_LIVE_DB = "test_nrf_impact_levy_go_live"
EDP_ID = 1
UNITS = 10


@pytest.fixture(scope="module")
def engine() -> Iterator[Engine]:
    with scratch_database(MIGRATION_DB) as engine:
        run_alembic(MIGRATION_DB, "upgrade", "head")
        yield engine


@pytest.fixture
def go_live_engine() -> Iterator[Engine]:
    with scratch_database(GO_LIVE_DB) as engine:
        run_alembic(GO_LIVE_DB, "upgrade", "head")
        yield engine


def _price(engine: Engine, at: datetime):
    with Session(engine) as session:
        return price_levy(session, EDP_ID, UNITS, at)


@pytest.mark.parametrize(
    ("at", "years"),
    [
        (datetime(2026, 11, 16, 12, tzinfo=UTC), [2026]),
        (datetime(2027, 6, 24, 12, tzinfo=UTC), [2026, 2027]),
    ],
)
def test_seeded_years_are_not_inflated(engine: Engine, at: datetime, years: list[int]):
    levy = _price(engine, at)

    assert levy.provisional_amount == Decimal("26750.00")
    assert levy.inflation_adjusted_amount == Decimal("26750.00")
    assert levy.yearly_lines[0].index_value == Decimal("400")
    assert [line.year for line in levy.yearly_lines] == years


CUTOFF = datetime(2027, 1, 1, tzinfo=UTC)
STAFF = {
    "comment": "go-live: 2027 is the base year",
    "requestor": "Grace Hopper",
    "jiraTicket": "NRF2-1294",
    "staffId": "entra-oid",
    "staffFullName": "Ada Admin",
    "staffEmail": "ada@example.com",
}


@contextmanager
def _staff_apis(engine: Engine, now: datetime) -> Iterator[TestClient]:
    """The staff APIs on `engine`, with their clock stopped at `now`."""

    class _Stopped(datetime):
        @classmethod
        def now(cls, tz=None) -> datetime:
            return now.astimezone(tz)

    repository = Repository(engine)
    with (
        patch("app.base_prices.router._get_repository", return_value=repository),
        patch("app.inflation_indexes.router._get_repository", return_value=repository),
        patch("app.base_prices.router.datetime", _Stopped),
        patch("app.inflation_indexes.router.datetime", _Stopped),
    ):
        yield TestClient(app)


def _add_2027_base_price(client: TestClient):
    return client.post(
        "/base-prices",
        json={
            "edpId": EDP_ID,
            "basePrice": 2675.00,
            "effectiveFrom": "2027-01-01T00:00:00Z",
        }
        | STAFF,
    )


def _update_2027_index(client: TestClient, cil_index: float):
    [index] = [
        i for i in client.get("/inflation-indexes").json() if i["chargingYear"] == 2027
    ]
    return client.patch(
        f"/inflation-indexes/{index['id']}", json={"cilIndex": cil_index}
    )


def test_go_live_steps_before_1_january_keep_2027_uninflated(go_live_engine: Engine):
    with _staff_apis(go_live_engine, datetime(2026, 12, 31, 23, 59, tzinfo=UTC)) as api:
        assert _add_2027_base_price(api).status_code == 201
        assert _update_2027_index(api, 412).status_code == 200

    december = _price(go_live_engine, datetime(2026, 12, 31, 23, 59, 59, tzinfo=UTC))
    june = _price(go_live_engine, datetime(2027, 6, 24, 12, tzinfo=UTC))

    assert december.inflation_adjusted_amount == Decimal("26750.00")
    assert [line.year for line in december.yearly_lines] == [2026]
    assert june.provisional_amount == Decimal("26750.00")
    assert june.inflation_adjusted_amount == Decimal("26750.00")
    assert [line.year for line in june.yearly_lines] == [2027]
    assert june.yearly_lines[0].index_value == Decimal("412")


def test_go_live_steps_are_refused_from_1_january(engine: Engine):
    with _staff_apis(engine, CUTOFF) as api:
        base_price = _add_2027_base_price(api)
        index = _update_2027_index(api, 412)

    assert base_price.status_code == 422
    assert base_price.json()["detail"] == "effectiveFrom must be in the future"
    assert index.status_code == 409
    assert index.json()["detail"]["error"] == "inflation_index_taken_effect"
