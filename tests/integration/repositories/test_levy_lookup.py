"""The levy lookups and audit record against PostGIS: id resolution reads the
active edp_boundary_layer version, the price lookup returns the base price in
force at a moment, and a recorded calculation reads back with every field
intact and names the base price and index rows it used. An index row that
priced a quote cannot be deleted; a base price can. A base price that is not
positive pounds and pence is rejected by the table."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DataError, IntegrityError

from app.repositories.levy import (
    get_levy_pricing_rows,
    resolve_edp_id,
)
from app.repositories.repository import Repository
from tests.integration.conftest import (
    insert_base_price,
    insert_edp_boundary,
    insert_inflation_index,
    set_active_version,
)

pytestmark = pytest.mark.integration

LABEL = "Test EDP"
WKT = (
    "MULTIPOLYGON(((600000 300000, 601000 300000, 601000 301000, "
    "600000 301000, 600000 300000)))"
)


def test_resolves_id_from_active_version_only(repository: Repository):
    insert_edp_boundary(repository, LABEL, WKT, edp_id=7, version=1)
    insert_edp_boundary(repository, LABEL, WKT, edp_id=8, version=2)
    set_active_version(repository, "edp_boundary_layer", 2)

    with repository.session() as session:
        assert resolve_edp_id(session, LABEL) == 8


def test_no_edp_id_attribute_means_none(repository: Repository):
    insert_edp_boundary(repository, LABEL, WKT, edp_id=None)

    with repository.session() as session:
        assert resolve_edp_id(session, LABEL) is None


def test_polygons_sharing_a_label_and_id_resolve(repository: Repository):
    insert_edp_boundary(repository, LABEL, WKT, edp_id=7)
    insert_edp_boundary(repository, LABEL, WKT, edp_id=7)

    with repository.session() as session:
        assert resolve_edp_id(session, LABEL) == 7


def test_a_label_with_two_ids_means_none(repository: Repository):
    insert_edp_boundary(repository, LABEL, WKT, edp_id=7)
    insert_edp_boundary(repository, LABEL, WKT, edp_id=8)

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
        "staff_id, staff_full_name, staff_email) VALUES "
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
    ("staff_id", 36),
    ("staff_full_name", 255),
    ("staff_email", 254),
]


def _insert_base_price_with(repository: Repository, column: str, value: str) -> None:
    values = {
        "comment": "Test",
        "staff_id": "staff-id",
        "staff_full_name": "Test Staff",
        "staff_email": "test.user@example.com",
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
                "(base_price, edp_id, effective_from, comment, staff_id, "
                "staff_full_name, staff_email) VALUES "
                "(2675, 1, now(), :comment, :staff_id, :staff_full_name, "
                ":staff_email)"
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


@pytest.mark.parametrize("cil_index", ["0", "-1", "NaN"])
def test_an_inflation_index_that_is_not_a_positive_number_is_rejected(
    repository: Repository, cil_index: str
):
    with pytest.raises(IntegrityError, match="ck_levy_inflation_index_cil_index"):
        insert_inflation_index(repository, 2030, cil_index=cil_index)


@pytest.mark.parametrize("comment", ["", "   ", "\t\n"])
def test_a_blank_inflation_index_comment_is_rejected(
    repository: Repository, comment: str
):
    with pytest.raises(
        IntegrityError, match="ck_levy_inflation_index_comment_not_blank"
    ):
        insert_inflation_index(repository, 2030, comment=comment)


# --- get_levy_pricing_rows ---------------------------------------------------


def test_pricing_rows_join_the_edp_start_and_calculation_year_indices(
    repository: Repository,
):
    insert_base_price(repository, 7)
    start_id = insert_inflation_index(repository, 2026, index_factor="1.0000")
    calc_id = insert_inflation_index(repository, 2027, index_factor="1.0500")

    with repository.session() as session:
        rows = get_levy_pricing_rows(session, 7, datetime(2027, 3, 1, tzinfo=UTC))

    assert rows.charge.edp_id == 7
    assert rows.charge.original_base_charge_per_unit == Decimal("2675")
    assert rows.charge.base_charge_per_unit == Decimal("2675")
    assert rows.edp_start_year_index.id == start_id
    assert rows.calculation_year_index.id == calc_id


def test_pricing_rows_leave_a_missing_index_none(repository: Repository):
    insert_base_price(repository, 7)
    start_id = insert_inflation_index(repository, 2026, index_factor="1.0000")

    with repository.session() as session:
        rows = get_levy_pricing_rows(session, 7, datetime(2027, 3, 1, tzinfo=UTC))

    assert rows.edp_start_year_index.id == start_id
    assert rows.calculation_year_index is None


def test_pricing_rows_ignore_an_index_year_that_has_not_begun(
    repository: Repository,
):
    # A saved future index must not leak into an earlier calculation: the
    # join is on the calculation year, so the 2028 row only applies from
    # 1 January 2028 (NRF2-1348 scenario 9).
    insert_base_price(repository, 7)
    start_id = insert_inflation_index(repository, 2026, index_factor="1.0000")
    insert_inflation_index(repository, 2028, index_factor="1.0512")

    with repository.session() as session:
        rows = get_levy_pricing_rows(session, 7, datetime(2026, 6, 1, tzinfo=UTC))

    assert rows.edp_start_year_index.id == start_id
    assert rows.calculation_year_index.id == start_id


def test_pricing_rows_need_a_base_price_in_force(repository: Repository):
    insert_base_price(repository, 7, datetime(2026, 1, 1, tzinfo=UTC))
    insert_inflation_index(repository, 2026, index_factor="1.0000")

    with repository.session() as session:
        in_force = datetime(2026, 9, 14, tzinfo=UTC)
        assert get_levy_pricing_rows(session, 7, in_force) is not None
        too_early = datetime(2025, 12, 31, 23, 59, 59, tzinfo=UTC)
        assert get_levy_pricing_rows(session, 7, too_early) is None
        assert get_levy_pricing_rows(session, 99, in_force) is None
