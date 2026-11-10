"""The levy lookups and audit record against PostGIS: id resolution reads the
active edp_boundary_layer version, the price lookup returns the base price in
force at a moment, and a recorded calculation reads back with every field
intact and names the base price and index rows it used. An index row that
priced a quote cannot be deleted; a base price can. A base price that is not
positive pounds and pence is rejected by the table."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import DataError, IntegrityError

from app.calculators.levy import MAX_LEVY_AMOUNT, MAX_LEVY_UNITS
from app.models.db import LevyCalculationRecord
from app.repositories.levy import (
    get_levy_pricing_rows,
    mark_levy_calculation_sent,
    record_levy_calculation,
    resolve_edp_id,
)
from app.repositories.repository import Repository
from tests.conftest import make_levy_calculation
from tests.integration.conftest import insert_base_price, set_active_version

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


def test_polygons_sharing_a_label_and_id_resolve(repository: Repository):
    _insert_edp(repository, LABEL, edp_id=7)
    _insert_edp(repository, LABEL, edp_id=7)

    with repository.session() as session:
        assert resolve_edp_id(session, LABEL) == 7


def test_a_label_with_two_ids_means_none(repository: Repository):
    _insert_edp(repository, LABEL, edp_id=7)
    _insert_edp(repository, LABEL, edp_id=8)

    with repository.session() as session:
        assert resolve_edp_id(session, LABEL) is None


def test_the_latest_base_price_at_or_before_the_moment_is_used(
    repository: Repository,
):
    first = insert_base_price(repository, 7, datetime(2026, 1, 1, tzinfo=UTC))
    second = insert_base_price(repository, 7, datetime(2026, 6, 1, 12, tzinfo=UTC))
    insert_base_price(repository, 8, datetime(2026, 3, 1, tzinfo=UTC))

    with repository.session() as session:
        before_change = get_levy_pricing_rows(
            session, 7, datetime(2026, 6, 1, 11, 59, 59, tzinfo=UTC)
        )
        at_change = get_levy_pricing_rows(
            session, 7, datetime(2026, 6, 1, 12, tzinfo=UTC)
        )

    assert before_change.charge.id == first
    assert at_change.charge.id == second


def test_two_base_prices_for_an_edp_cannot_share_an_effective_from(
    repository: Repository,
):
    effective_from = datetime(2026, 1, 1, tzinfo=UTC)
    insert_base_price(repository, 7, effective_from)

    with pytest.raises(
        IntegrityError, match="ix_public_base_prices_edp_id_effective_from"
    ):
        insert_base_price(repository, 7, effective_from)


def test_a_base_price_for_an_unknown_edp_is_rejected(repository: Repository):
    insert = text(
        "INSERT INTO public.base_prices "
        "(base_price, edp_id, effective_from, comment, "
        "user_entra_id, user_full_name, user_email) VALUES "
        "(2675, 99, now(), 'Test', 'id', 'Name', "
        "'name@example.com')"
    )
    with (
        repository.session() as session,
        pytest.raises(IntegrityError, match="fk_base_prices_edp"),
    ):
        session.execute(insert)


@pytest.mark.parametrize("price", ["0", "-2675", "NaN", "Infinity"])
def test_a_base_price_that_is_not_a_positive_number_is_rejected(
    repository: Repository, price: str
):
    with pytest.raises(IntegrityError, match="ck_base_prices_base_price"):
        insert_base_price(repository, price=price)


def test_a_base_price_beyond_the_column_range_is_rejected(repository: Repository):
    with pytest.raises(DataError, match="numeric field overflow"):
        insert_base_price(repository, price="100000000")


@pytest.mark.parametrize("price", ["0.01", "2675.55", "2675.5000", "99999999.99"])
def test_a_base_price_in_pounds_and_pence_is_stored_unchanged(
    repository: Repository, price: str
):
    insert_base_price(repository, price=price)

    with repository.session() as session:
        stored = session.execute(
            text("SELECT base_price FROM public.base_prices")
        ).scalar_one()
    assert stored == Decimal(price)


_TEXT_COLUMN_LIMITS = [
    ("comment", 500),
    ("user_entra_id", 36),
    ("user_full_name", 255),
    ("user_email", 254),
]


def _insert_base_price_with(repository: Repository, column: str, value: str) -> None:
    values = {
        "comment": "Test",
        "user_entra_id": "entra-id",
        "user_full_name": "Test User",
        "user_email": "test.user@example.com",
    } | {column: value}
    with repository.session() as session:
        session.execute(
            text(
                "INSERT INTO public.edps (id, name, start_date) "
                "VALUES (1, 'Test EDP', DATE '2026-01-01') ON CONFLICT DO NOTHING"
            )
        )
        session.execute(
            text(
                "INSERT INTO public.base_prices "
                "(base_price, edp_id, effective_from, comment, user_entra_id, "
                "user_full_name, user_email) VALUES "
                "(2675, 1, now(), :comment, :user_entra_id, :user_full_name, "
                ":user_email)"
            ),
            values,
        )
        session.commit()


@pytest.mark.parametrize(("column", "limit"), _TEXT_COLUMN_LIMITS)
def test_a_text_column_over_its_limit_is_rejected(
    repository: Repository, column: str, limit: int
):
    with pytest.raises(DataError, match="value too long for type character varying"):
        _insert_base_price_with(repository, column, "x" * (limit + 1))


@pytest.mark.parametrize(("column", "limit"), _TEXT_COLUMN_LIMITS)
def test_a_text_column_at_its_limit_is_stored(
    repository: Repository, column: str, limit: int
):
    _insert_base_price_with(repository, column, "x" * limit)

    with repository.session() as session:
        stored = session.execute(
            # column comes from the constant _TEXT_COLUMN_LIMITS
            text(f"SELECT {column} FROM public.base_prices")  # noqa: S608
        ).scalar_one()
    assert len(stored) == limit


@pytest.mark.parametrize("comment", ["", "   ", "\t\n"])
def test_a_blank_comment_is_rejected(repository: Repository, comment: str):
    with pytest.raises(IntegrityError, match="ck_base_prices_comment_not_blank"):
        _insert_base_price_with(repository, "comment", comment)


def _insert_index(
    repository: Repository, year: int, factor: str, cil_index: str = "400"
) -> UUID:
    with repository.session() as session:
        index_id = session.execute(
            text(
                "INSERT INTO public.levy_inflation_index "
                "(id, charging_year, cil_index, index_factor) VALUES "
                "(gen_random_uuid(), :year, :cil_index, :factor) RETURNING id"
            ),
            {"year": year, "cil_index": cil_index, "factor": factor},
        ).scalar_one()
        session.commit()
    return index_id


# --- get_levy_pricing_rows ---------------------------------------------------


def test_pricing_rows_join_the_edp_start_and_calculation_year_indices(
    repository: Repository,
):
    base_price_id = insert_base_price(repository, 7)
    start_id = _insert_index(repository, 2026, "1.0000")
    calc_id = _insert_index(repository, 2027, "1.0500")

    with repository.session() as session:
        rows = get_levy_pricing_rows(session, 7, datetime(2027, 3, 1, tzinfo=UTC))

    assert rows.charge.id == base_price_id
    assert rows.charge.edp_id == 7
    assert rows.charge.edp_name == "Test EDP"
    assert rows.charge.base_charge_per_unit == Decimal("2675")
    assert rows.edp_start_year_index.id == start_id
    assert rows.calculation_year_index.id == calc_id


def test_pricing_rows_leave_a_missing_index_none(repository: Repository):
    insert_base_price(repository, 7)
    start_id = _insert_index(repository, 2026, "1.0000")

    with repository.session() as session:
        rows = get_levy_pricing_rows(session, 7, datetime(2027, 3, 1, tzinfo=UTC))

    assert rows.edp_start_year_index.id == start_id
    assert rows.calculation_year_index is None


def test_pricing_rows_need_a_base_price_in_force(repository: Repository):
    insert_base_price(repository, 7, datetime(2026, 1, 1, tzinfo=UTC))
    _insert_index(repository, 2026, "1.0000")

    with repository.session() as session:
        in_force = datetime(2026, 9, 14, tzinfo=UTC)
        assert get_levy_pricing_rows(session, 7, in_force) is not None
        too_early = datetime(2025, 12, 31, 23, 59, 59, tzinfo=UTC)
        assert get_levy_pricing_rows(session, 7, too_early) is None
        assert get_levy_pricing_rows(session, 99, in_force) is None


# --- record_levy_calculation -------------------------------------------------


def test_record_levy_calculation_round_trips(repository: Repository):
    charge_id = insert_base_price(repository, 1)
    start_id = _insert_index(repository, 2026, "1.0000")
    calc_id = _insert_index(repository, 2028, "1.0512")
    levy = make_levy_calculation(
        levy_charge_id=charge_id,
        edp_start_year_index_id=start_id,
        edp_start_year_index_factor=Decimal("1.0000"),
        calculation_year_index_id=calc_id,
        calculation_year_index_factor=Decimal("1.0512"),
    )
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
    assert rows[0].base_charge_per_unit == Decimal("2675.0000")
    assert rows[0].provisional_amount == Decimal("26750.00")
    assert rows[0].created_at is not None
    assert rows[0].levy_charge_id == charge_id
    assert rows[0].edp_start_year_index_id == start_id
    assert rows[0].edp_start_year_index_factor == Decimal("1.0000")
    assert rows[0].calculation_year_index_id == calc_id
    assert rows[0].calculation_year_index_factor == Decimal("1.0512")


def test_record_without_inflation_leaves_index_columns_null(repository: Repository):
    charge_id = insert_base_price(repository, 1)
    with repository.session() as session:
        record_levy_calculation(
            session, "NRL-000002", make_levy_calculation(levy_charge_id=charge_id)
        )
        session.commit()

    with repository.session() as session:
        row = session.scalars(
            select(LevyCalculationRecord).where(
                LevyCalculationRecord.quote_reference == "NRL-000002"
            )
        ).one()

    assert row.edp_start_year_index_id is None
    assert row.calculation_year_index_id is None


def test_mark_sent_stamps_only_the_named_row(repository: Repository):
    charge_id = insert_base_price(repository, 1)
    levy = make_levy_calculation(levy_charge_id=charge_id)
    with repository.session() as session:
        # A redelivered job records a second row for the same quote.
        first = record_levy_calculation(session, "NRL-000005", levy)
        second = record_levy_calculation(session, "NRL-000005", levy)
        session.commit()
        assert first.sent_at is None

    with repository.session() as session:
        mark_levy_calculation_sent(session, second.id)
        session.commit()

    with repository.session() as session:
        sent = {
            row.id: row.sent_at
            for row in session.scalars(
                select(LevyCalculationRecord).where(
                    LevyCalculationRecord.quote_reference == "NRL-000005"
                )
            )
        }

    assert sent[first.id] is None
    assert sent[second.id] is not None


def test_record_holds_the_largest_levy_the_calculator_prices(repository: Repository):
    charge_id = insert_base_price(repository, price="99999999.99")
    with repository.session() as session:
        record_levy_calculation(
            session,
            "NRL-000006",
            make_levy_calculation(
                levy_charge_id=charge_id,
                base_charge_per_unit=Decimal("99999999.99"),
                provisional_amount=MAX_LEVY_AMOUNT,
                inflation_adjusted_amount=MAX_LEVY_AMOUNT,
            ),
        )
        session.commit()

    with repository.session() as session:
        row = session.scalars(
            select(LevyCalculationRecord).where(
                LevyCalculationRecord.quote_reference == "NRL-000006"
            )
        ).one()

    assert row.provisional_amount == MAX_LEVY_AMOUNT
    assert row.inflation_adjusted_amount == MAX_LEVY_AMOUNT
    assert row.base_charge_per_unit == Decimal("99999999.99")


def test_record_holds_the_most_units_the_calculator_prices(repository: Repository):
    charge_id = insert_base_price(repository, price="0.01")
    with repository.session() as session:
        record_levy_calculation(
            session,
            "NRL-000007",
            make_levy_calculation(levy_charge_id=charge_id, units=MAX_LEVY_UNITS),
        )
        session.commit()

    with repository.session() as session:
        row = session.scalars(
            select(LevyCalculationRecord).where(
                LevyCalculationRecord.quote_reference == "NRL-000007"
            )
        ).one()

    assert row.units == MAX_LEVY_UNITS


def test_record_rejects_one_more_unit_than_the_calculator_prices(
    repository: Repository,
):
    charge_id = insert_base_price(repository, price="0.01")
    levy = make_levy_calculation(levy_charge_id=charge_id, units=MAX_LEVY_UNITS + 1)
    with repository.session() as session:
        record_levy_calculation(session, "NRL-000008", levy)
        with pytest.raises(DataError, match="out of range"):
            session.flush()


def test_record_survives_the_base_price_being_deleted(repository: Repository):
    base_price_id = insert_base_price(repository, 1)
    with repository.session() as session:
        record_levy_calculation(
            session,
            "NRL-000003",
            make_levy_calculation(levy_charge_id=base_price_id),
        )
        session.commit()

    with repository.session() as session:
        session.execute(text("DELETE FROM public.base_prices"))
        session.commit()

    with repository.session() as session:
        row = session.scalars(
            select(LevyCalculationRecord).where(
                LevyCalculationRecord.quote_reference == "NRL-000003"
            )
        ).one()
    assert row.levy_charge_id == base_price_id


def test_an_index_row_that_priced_a_quote_cannot_be_deleted(
    repository: Repository,
):
    charge_id = insert_base_price(repository, 1)
    start_id = _insert_index(repository, 2026, "1.0000")
    calc_id = _insert_index(repository, 2028, "1.0512")
    levy = make_levy_calculation(
        levy_charge_id=charge_id,
        edp_start_year_index_id=start_id,
        edp_start_year_index_factor=Decimal("1.0000"),
        calculation_year_index_id=calc_id,
        calculation_year_index_factor=Decimal("1.0512"),
    )
    with repository.session() as session:
        record_levy_calculation(session, "NRL-000004", levy)
        session.commit()

    delete = text("DELETE FROM public.levy_inflation_index")
    with repository.session() as session:
        with pytest.raises(IntegrityError, match="fk_audit_levy_calculations"):
            session.execute(delete)
        session.rollback()
