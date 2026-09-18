"""The levy lookups and audit record against PostGIS: id resolution reads the
active edp_boundary_layer version, the charge lookup honours the validity
window, and a recorded calculation reads back with every field intact."""

from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import select, text

from app.models.db import LevyCalculationRecord
from app.repositories.levy import (
    get_inflation_index,
    get_levy_charge,
    record_levy_calculation,
    resolve_edp_id,
)
from app.repositories.repository import Repository
from tests.conftest import make_levy_calculation
from tests.integration.conftest import set_active_version

pytestmark = pytest.mark.integration

LABEL = "Test EDP"
WKT = (
    "MULTIPOLYGON(((600000 300000, 601000 300000, 601000 301000, "
    "600000 301000, 600000 300000)))"
)


_INSERT_EDP_WITH_ID = (
    "INSERT INTO public.edp_boundary_layer "
    "(id, version, geometry, name, attributes) VALUES "
    "(gen_random_uuid(), :v, ST_GeomFromText(:wkt, 27700), :n, "
    "jsonb_build_object('EDP_Name', :n, 'EDP_Id', CAST(:id AS integer)))"
)
_INSERT_EDP_WITHOUT_ID = (
    "INSERT INTO public.edp_boundary_layer "
    "(id, version, geometry, name, attributes) VALUES "
    "(gen_random_uuid(), :v, ST_GeomFromText(:wkt, 27700), :n, "
    "jsonb_build_object('EDP_Name', :n))"
)


def _insert_edp(repository: Repository, label: str, edp_id, version: int = 1) -> None:
    params = {"v": version, "wkt": WKT, "n": label}
    if edp_id is None:
        sql = _INSERT_EDP_WITHOUT_ID
    else:
        sql = _INSERT_EDP_WITH_ID
        params["id"] = edp_id
    with repository.session() as session:
        session.execute(text(sql), params)
        session.commit()


def _insert_charge(
    repository: Repository, edp_id: int, valid_from: date, valid_to: date
) -> None:
    with repository.session() as session:
        session.execute(
            text(
                "INSERT INTO public.levy_charges "
                "(id, edp_id, edp_name, edp_start_date, charge_valid_from, "
                "charge_valid_to, base_charge_per_unit) VALUES "
                "(gen_random_uuid(), :id, 'x', :f, :f, :t, 100.1234)"
            ),
            {"id": edp_id, "f": valid_from, "t": valid_to},
        )
        session.commit()


def test_resolves_id_from_active_version_only(repository: Repository):
    _insert_edp(repository, LABEL, edp_id=7, version=1)
    _insert_edp(repository, LABEL, edp_id=8, version=2)
    set_active_version(repository, "edp_boundary_layer", 2)

    with repository.session() as session:
        assert resolve_edp_id(session, LABEL) == 8


def test_no_edp_id_attribute_means_none(repository: Repository):
    _insert_edp(repository, LABEL, edp_id=None)

    with repository.session() as session:
        assert resolve_edp_id(session, LABEL) is None


def test_charge_lookup_honours_validity_window(repository: Repository):
    _insert_charge(repository, 7, date(2026, 1, 1), date(2027, 12, 31))

    with repository.session() as session:
        assert get_levy_charge(session, 7, date(2026, 9, 14)) is not None
        assert get_levy_charge(session, 7, date(2028, 1, 1)) is None
        assert get_levy_charge(session, 99, date(2026, 9, 14)) is None


# --- get_inflation_index ------------------------------------------------------


def _insert_index(
    repository: Repository, year: int, factor: str, cil_index: str = "400"
) -> None:
    with repository.session() as session:
        session.execute(
            text(
                "INSERT INTO public.levy_inflation_index "
                "(id, charging_year, cil_index, index_factor) VALUES "
                "(gen_random_uuid(), :year, :cil_index, :factor)"
            ),
            {"year": year, "cil_index": cil_index, "factor": factor},
        )
        session.commit()


def test_inflation_index_lookup_by_charging_year(repository: Repository):
    _insert_index(repository, 2026, "1.0000")

    with repository.session() as session:
        assert get_inflation_index(session, 2026) == Decimal("1.0000")
        assert get_inflation_index(session, 2099) is None


# --- record_levy_calculation -------------------------------------------------


def test_record_levy_calculation_round_trips(repository: Repository):
    levy = make_levy_calculation()
    with repository.session() as session:
        record_levy_calculation(session, "NRL-000001", levy)
        session.commit()

    with repository.session() as session:
        rows = session.scalars(
            select(LevyCalculationRecord).where(
                LevyCalculationRecord.quote_reference == "NRL-000001"
            )
        ).all()

    assert len(rows) == 1
    assert rows[0].base_charge_per_unit == Decimal("2193.6649")
    assert rows[0].provisional_amount == Decimal("21936.60")
    assert rows[0].created_at is not None
