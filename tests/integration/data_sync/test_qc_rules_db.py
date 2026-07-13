"""DB-backed checks that the generated QC SQL is valid and evaluates correctly
against real PostGIS functions (ST_IsValid, ST_SRID, GeometryType) and real
numeric comparisons — the unit tests in test_qc.py only assert on the
generated SQL string, not that Postgres accepts and runs it.
"""

from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.exc import InternalError

from app.data_sync.qc import build_qc_sql
from app.data_sync.qc_rules import (
    CoefficientRange,
    GeometryRule,
    KeyRule,
    QcRules,
    TableRules,
)

pytestmark = pytest.mark.integration

_VALID_POLYGON_WKT = "SRID=27700;POLYGON((0 0,0 10,10 10,10 0,0 0))"
_INVALID_POLYGON_WKT = (
    "SRID=27700;POLYGON((0 0,10 10,0 10,10 0,0 0))"  # self-intersecting
)
_WRONG_SRID_WKT = "SRID=4326;POLYGON((0 0,0 10,10 10,10 0,0 0))"


def _run_qc(conn, table: str, rules: TableRules):
    qc_rules = QcRules(tables={table: rules})
    sql = build_qc_sql([(table, Path("dummy.gz"))], qc_rules)
    conn.execute(text(sql))


def test_geometry_rule_passes_on_valid_geometry(test_engine):
    with test_engine.connect() as conn:
        conn.execute(
            text(
                "CREATE TEMP TABLE _ds_stage_wwtw_catchments "
                "(geometry geometry(Polygon, 27700))"
            )
        )
        conn.execute(
            text(
                "INSERT INTO pg_temp._ds_stage_wwtw_catchments (geometry) "  # noqa: S608
                f"VALUES (ST_GeomFromEWKT('{_VALID_POLYGON_WKT}'))"
            )
        )
        rules = TableRules(
            geometry=GeometryRule(expected_type="Polygon", expected_srid=27700)
        )
        _run_qc(conn, "wwtw_catchments", rules)  # must not raise


def test_geometry_rule_tolerates_repairable_invalid_geometry(test_engine):
    """A self-intersecting ("bowtie") polygon is technically invalid per
    ST_IsValid, but PostGIS's ST_MakeValid repairs it — rule 4 (see
    docs/data-management.md) explicitly treats repairable geometry as passing,
    not failing. This proves the QC gate's repair-tolerance works as designed,
    not merely that it doesn't crash on invalid input.
    """
    with test_engine.connect() as conn:
        conn.execute(
            text(
                "CREATE TEMP TABLE _ds_stage_wwtw_catchments "
                "(geometry geometry(Polygon, 27700))"
            )
        )
        conn.execute(
            text(
                "INSERT INTO pg_temp._ds_stage_wwtw_catchments (geometry) "  # noqa: S608
                f"VALUES (ST_GeomFromEWKT('{_INVALID_POLYGON_WKT}'))"
            )
        )
        rules = TableRules(
            geometry=GeometryRule(expected_type="Polygon", expected_srid=27700)
        )
        _run_qc(
            conn, "wwtw_catchments", rules
        )  # must not raise — ST_MakeValid repairs it


def test_geometry_rule_fails_on_wrong_srid(test_engine):
    with test_engine.connect() as conn:
        conn.execute(
            text(
                "CREATE TEMP TABLE _ds_stage_wwtw_catchments "
                "(geometry geometry(Polygon))"
            )
        )
        conn.execute(
            text(
                "INSERT INTO pg_temp._ds_stage_wwtw_catchments (geometry) "  # noqa: S608
                f"VALUES (ST_GeomFromEWKT('{_WRONG_SRID_WKT}'))"
            )
        )
        rules = TableRules(
            geometry=GeometryRule(expected_type="Polygon", expected_srid=27700)
        )
        with pytest.raises(Exception, match="geometry_srid"):
            _run_qc(conn, "wwtw_catchments", rules)


def test_coefficient_range_rule_passes_within_bounds(test_engine):
    with test_engine.connect() as conn:
        conn.execute(
            text(
                "CREATE TEMP TABLE _ds_stage_coefficient_layer (lu_curr_n_coeff float8)"
            )
        )
        conn.execute(
            text("INSERT INTO pg_temp._ds_stage_coefficient_layer VALUES (25.0)")
        )
        rules = TableRules(
            coefficient_ranges={"lu_curr_n_coeff": CoefficientRange(min=0, max=50)}
        )
        _run_qc(conn, "coefficient_layer", rules)  # must not raise


def test_coefficient_range_rule_fails_outside_bounds(test_engine):
    with test_engine.connect() as conn:
        conn.execute(
            text(
                "CREATE TEMP TABLE _ds_stage_coefficient_layer (lu_curr_n_coeff float8)"
            )
        )
        conn.execute(
            text("INSERT INTO pg_temp._ds_stage_coefficient_layer VALUES (999.0)")
        )
        rules = TableRules(
            coefficient_ranges={"lu_curr_n_coeff": CoefficientRange(min=0, max=50)}
        )
        with pytest.raises(Exception, match="coefficient_range"):
            _run_qc(conn, "coefficient_layer", rules)


def test_allowed_values_rule_fails_on_value_outside_set(test_engine):
    """Regression test for the DM-1 bug where the generated `format('...')`
    call embedded unescaped single quotes from the allowed-values list inside
    an outer single-quoted string literal, producing a PostgreSQL syntax
    error on every reload of a table with an `allowed_values` rule —
    regardless of whether the staged data was actually valid. Before the fix,
    this test's `_run_qc` call raised a syntax error ("syntax error at or
    near \"Red\"") rather than the expected `rule=allowed_values` failure.
    """
    with test_engine.connect() as conn:
        conn.execute(
            text("CREATE TEMP TABLE _ds_stage_gcn_risk_zones (attributes jsonb)")
        )
        conn.execute(
            text(
                "INSERT INTO pg_temp._ds_stage_gcn_risk_zones (attributes) "
                'VALUES (\'{"RZ": "Purple"}\'::jsonb)'
            )
        )
        rules = TableRules(
            key=KeyRule(columns=["attributes.RZ"], source="json", unique=False),
            allowed_values={"attributes.RZ": ["Red", "Amber", "Green"]},
        )
        with pytest.raises(Exception, match="allowed_values"):
            _run_qc(conn, "gcn_risk_zones", rules)


def test_row_count_rule_hard_fails_on_zero_rows(test_engine):
    with test_engine.connect() as conn:
        conn.execute(text("CREATE TEMP TABLE _ds_stage_gcn_ponds (id uuid)"))
        rules = TableRules()
        with pytest.raises(Exception, match="row_count"):
            _run_qc(conn, "gcn_ponds", rules)


def test_multiple_failures_are_aggregated_not_fail_fast(test_engine):
    """One failing row per rule across two tables both surface in one error."""
    with test_engine.connect() as conn:
        conn.execute(text("CREATE TEMP TABLE _ds_stage_gcn_ponds (id uuid)"))
        conn.execute(
            text(
                "CREATE TEMP TABLE _ds_stage_coefficient_layer (lu_curr_n_coeff float8)"
            )
        )
        conn.execute(
            text("INSERT INTO pg_temp._ds_stage_coefficient_layer VALUES (999.0)")
        )
        qc_rules = QcRules(
            tables={
                "gcn_ponds": TableRules(),
                "coefficient_layer": TableRules(
                    coefficient_ranges={
                        "lu_curr_n_coeff": CoefficientRange(min=0, max=50)
                    }
                ),
            }
        )
        sql = build_qc_sql(
            [("gcn_ponds", Path("a.gz")), ("coefficient_layer", Path("b.gz"))], qc_rules
        )
        stmt = text(sql)
        with pytest.raises(InternalError) as exc_info:
            conn.execute(stmt)
        message = str(exc_info.value)
        assert "table=gcn_ponds rule=row_count" in message
        assert "table=coefficient_layer rule=coefficient_range" in message
