"""The levy lookups against PostGIS: id resolution reads the
active edp_boundary_layer version, the price lookup returns the base price in
force at a moment. A base price that is not positive pounds and pence is
rejected by the table."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import DataError, IntegrityError

from app.repositories.levy import (
    get_levy_pricing_rows,
    levy_pricing_statement,
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
UNITS = 10
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
    insert_base_price(repository, 7, datetime(2026, 1, 1, tzinfo=UTC), price="2675")
    insert_base_price(repository, 7, datetime(2026, 6, 1, 12, tzinfo=UTC), price="3000")
    insert_base_price(repository, 8, datetime(2026, 3, 1, tzinfo=UTC), price="1000")

    with repository.session() as session:
        before_change = get_levy_pricing_rows(
            session, 7, UNITS, datetime(2026, 6, 1, 11, 59, 59, tzinfo=UTC)
        )
        at_change = get_levy_pricing_rows(
            session, 7, UNITS, datetime(2026, 6, 1, 12, tzinfo=UTC)
        )

    assert before_change.charge.base_price_per_unit == Decimal("2675")
    assert at_change.charge.base_price_per_unit == Decimal("3000")
    assert before_change.charge.original_base_price_per_unit == Decimal("2675")
    assert at_change.charge.original_base_price_per_unit == Decimal("2675")


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


def _years(rows) -> list[int]:
    return [i.charging_year for i in rows.indices]


def test_pricing_rows_return_the_index_for_each_year_from_the_base_year(
    repository: Repository,
):
    insert_base_price(repository, 7, datetime(2027, 1, 1, tzinfo=UTC))
    for year in (2031, 2026, 2028, 2027, 2030, 2029):
        insert_inflation_index(repository, year)

    with repository.session() as session:
        rows = get_levy_pricing_rows(
            session, 7, UNITS, datetime(2030, 3, 1, tzinfo=UTC)
        )

    assert rows.charge.edp_id == 7
    assert rows.charge.original_base_price_per_unit == Decimal("2675")
    assert rows.charge.base_price_per_unit == Decimal("2675")
    assert rows.charge.base_price_effective_from == datetime(2027, 1, 1, tzinfo=UTC)
    assert _years(rows) == [2027, 2028, 2029, 2030]


def test_pricing_rows_start_at_a_later_base_price(repository: Repository):
    insert_base_price(repository, 7, datetime(2027, 1, 1, tzinfo=UTC))
    insert_base_price(repository, 7, datetime(2029, 1, 1, tzinfo=UTC), price="3000")
    for year in range(2027, 2031):
        insert_inflation_index(repository, year)

    with repository.session() as session:
        rows = get_levy_pricing_rows(
            session, 7, UNITS, datetime(2030, 3, 1, tzinfo=UTC)
        )

    assert rows.charge.base_price_per_unit == Decimal("3000")
    assert rows.charge.original_base_price_per_unit == Decimal("2675")
    assert _years(rows) == [2029, 2030]


def test_pricing_rows_leave_a_gap_year_out(repository: Repository):
    insert_base_price(repository, 7, datetime(2027, 1, 1, tzinfo=UTC))
    insert_inflation_index(repository, 2027)
    insert_inflation_index(repository, 2029)

    with repository.session() as session:
        rows = get_levy_pricing_rows(
            session, 7, UNITS, datetime(2029, 3, 1, tzinfo=UTC)
        )

    assert _years(rows) == [2027, 2029]


def test_pricing_rows_need_a_base_price_in_force(repository: Repository):
    insert_base_price(repository, 7, datetime(2026, 1, 1, tzinfo=UTC))
    insert_inflation_index(repository, 2026, index_factor="1.0000")

    with repository.session() as session:
        in_force = datetime(2026, 9, 14, tzinfo=UTC)
        assert get_levy_pricing_rows(session, 7, UNITS, in_force) is not None
        too_early = datetime(2025, 12, 31, 23, 59, 59, tzinfo=UTC)
        assert get_levy_pricing_rows(session, 7, UNITS, too_early) is None
        assert get_levy_pricing_rows(session, 99, UNITS, in_force) is None


@pytest.mark.parametrize(("price", "years"), [("49.99", []), ("50.00", [2026])])
def test_pricing_rows_leave_the_indices_out_under_50_pounds(
    repository: Repository, price: str, years: list[int]
):
    insert_base_price(repository, 7, datetime(2026, 1, 1, tzinfo=UTC), price=price)
    insert_inflation_index(repository, 2026, index_factor="1.0000")

    with repository.session() as session:
        rows = get_levy_pricing_rows(session, 7, 1, datetime(2026, 9, 14, tzinfo=UTC))

    assert rows.charge.base_price_per_unit == Decimal(price)
    assert _years(rows) == years


def _index_scan_loops(plan: dict) -> list[int]:
    """Actual Loops of every plan node that reads levy_inflation_index."""
    loops = []
    if plan.get("Relation Name") == "levy_inflation_index":
        loops.append(plan["Actual Loops"])
    for child in plan.get("Plans", []):
        loops.extend(_index_scan_loops(child))
    return loops


@pytest.mark.parametrize(("price", "loops"), [("49.99", [0]), ("50.00", [1])])
def test_the_index_table_is_not_read_under_50_pounds(
    repository: Repository, price: str, loops: list[int]
):
    # Leaving the rows out is not enough: the scan itself must not run.
    insert_base_price(repository, 7, datetime(2026, 1, 1, tzinfo=UTC), price=price)
    insert_inflation_index(repository, 2026, index_factor="1.0000")
    stmt = levy_pricing_statement(7, 1, datetime(2026, 9, 14, tzinfo=UTC))

    with repository.session() as session:
        compiled = stmt.compile(dialect=session.bind.dialect)
        [[explained]] = (
            session.connection()
            .exec_driver_sql(
                f"EXPLAIN (ANALYZE, FORMAT JSON) {compiled}", compiled.params
            )
            .all()
        )

    assert _index_scan_loops(explained[0]["Plan"]) == loops


def test_pricing_rows_bind_more_units_than_a_bigint_holds(repository: Repository):
    insert_base_price(repository, 7, datetime(2026, 1, 1, tzinfo=UTC))
    insert_inflation_index(repository, 2026, index_factor="1.0000")

    with repository.session() as session:
        rows = get_levy_pricing_rows(
            session, 7, 10**24, datetime(2026, 9, 14, tzinfo=UTC)
        )

    assert _years(rows) == [2026]


def test_pricing_rows_are_read_in_one_statement(repository: Repository):
    # Two statements can see different snapshots under READ COMMITTED, so an
    # older base price could pair with an index saved after it.
    insert_base_price(repository, 7, datetime(2027, 1, 1, tzinfo=UTC))
    for year in range(2027, 2031):
        insert_inflation_index(repository, year)
    statements = []

    def record(conn, cursor, statement, *args):
        statements.append(statement)

    event.listen(repository.engine, "before_cursor_execute", record)
    try:
        with repository.session() as session:
            get_levy_pricing_rows(session, 7, UNITS, datetime(2030, 3, 1, tzinfo=UTC))
    finally:
        event.remove(repository.engine, "before_cursor_execute", record)

    assert len(statements) == 1
