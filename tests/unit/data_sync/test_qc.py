import pytest

from app.data_sync.qc import _row_count_sql
from app.data_sync.qc_rules import TableRules


def test_row_count_sql_checks_zero_and_floor():
    rules = TableRules()
    sql = _row_count_sql("nn_catchments", rules, floor_pct=90)
    assert "FROM pg_temp._ds_stage_nn_catchments" in sql
    assert "INTO staged_count" in sql
    assert "FROM public.nn_catchments" in sql
    assert "INTO prev_count" in sql
    assert "staged_count = 0" in sql
    assert "rule=row_count" in sql
    assert "staged_count < CEIL(prev_count * 0.9)" in sql


def test_row_count_sql_uses_per_table_override():
    rules = TableRules(row_count_floor_pct=50)
    sql = _row_count_sql("gcn_ponds", rules, floor_pct=90)
    assert "staged_count < CEIL(prev_count * 0.5)" in sql


def test_column_key_sql_checks_null_and_uniqueness():
    from app.data_sync.qc import _column_key_sql
    from app.data_sync.qc_rules import KeyRule, TableRules

    rules = TableRules(
        key=KeyRule(columns=["crome_id"], source="column", unique=True),
        non_null_columns=["land_use_cat", "nn_catchment", "subcatchment"],
    )
    sql = _column_key_sql("coefficient_layer", rules)
    assert "FROM pg_temp._ds_stage_coefficient_layer WHERE crome_id IS NULL" in sql
    assert "rule=key_not_null" in sql
    assert "GROUP BY crome_id HAVING COUNT(*) > 1" in sql
    assert "rule=key_unique" in sql
    assert "WHERE land_use_cat IS NULL" in sql
    assert "WHERE nn_catchment IS NULL" in sql
    assert "WHERE subcatchment IS NULL" in sql
    assert "rule=non_null" in sql


def test_column_key_sql_supports_composite_key_without_uniqueness_toggle():
    from app.data_sync.qc import _column_key_sql
    from app.data_sync.qc_rules import KeyRule, TableRules

    rules = TableRules(
        key=KeyRule(columns=["name", "version"], source="column", unique=True)
    )
    sql = _column_key_sql("lookup_table", rules)
    assert "WHERE name IS NULL OR version IS NULL" in sql
    assert "GROUP BY name, version HAVING COUNT(*) > 1" in sql


def test_column_key_sql_skips_uniqueness_check_when_not_unique():
    from app.data_sync.qc import _column_key_sql
    from app.data_sync.qc_rules import KeyRule, TableRules

    rules = TableRules(key=KeyRule(columns=["crome_id"], source="column", unique=False))
    sql = _column_key_sql("coefficient_layer", rules)
    assert "rule=key_not_null" in sql
    assert "rule=key_unique" not in sql


def test_json_key_sql_checks_null_and_uniqueness():
    from app.data_sync.qc import _json_key_sql
    from app.data_sync.qc_rules import KeyRule, TableRules

    rules = TableRules(
        key=KeyRule(columns=["attributes.OID"], source="json", unique=True),
        non_null_json_columns=["attributes.N2K_Site_N"],
    )
    sql = _json_key_sql("nn_catchments", rules)
    assert "attributes->>'OID' IS NULL" in sql
    assert "rule=key_not_null" in sql
    assert "GROUP BY attributes->>'OID' HAVING COUNT(*) > 1" in sql
    assert "rule=key_unique" in sql
    assert "attributes->>'N2K_Site_N' IS NULL" in sql
    assert "rule=non_null" in sql


def test_json_key_sql_non_unique_skips_uniqueness_check():
    from app.data_sync.qc import _json_key_sql
    from app.data_sync.qc_rules import KeyRule, TableRules

    rules = TableRules(
        key=KeyRule(columns=["attributes.NAME"], source="json", unique=False)
    )
    sql = _json_key_sql("lpa_boundaries", rules)
    assert "rule=key_unique" not in sql


def test_json_key_sql_allowed_values():
    from app.data_sync.qc import _json_key_sql
    from app.data_sync.qc_rules import KeyRule, TableRules

    rules = TableRules(
        key=KeyRule(columns=["attributes.RZ"], source="json", unique=False),
        allowed_values={"attributes.RZ": ["Red", "Amber", "Green"]},
    )
    sql = _json_key_sql("gcn_risk_zones", rules)
    assert "attributes->>'RZ' NOT IN ('Red', 'Amber', 'Green')" in sql
    assert "rule=allowed_values" in sql
    # The WHERE ... NOT IN clause is a standalone SQL context and keeps
    # single-escaped quotes, but the error-message text is embedded inside
    # an outer format('...') string literal, so its quotes must be doubled
    # to avoid terminating that literal early (PostgreSQL syntax error).
    assert "outside {''Red'', ''Amber'', ''Green''}" in sql


def test_json_key_sql_rejects_composite_json_key():
    from app.data_sync.qc import _json_key_sql
    from app.data_sync.qc_rules import KeyRule, TableRules

    rules = TableRules(
        key=KeyRule(
            columns=["attributes.A", "attributes.B"], source="json", unique=True
        )
    )
    with pytest.raises(ValueError, match="exactly one column"):
        _json_key_sql("some_table", rules)


def test_lookup_row_sql_checks_key_null_and_uniqueness_per_row():
    from app.data_sync.qc import _lookup_row_sql
    from app.data_sync.qc_rules import LookupRowRule, TableRules

    rules = TableRules(
        lookup_rows={
            "wwtw_lookup": LookupRowRule(json_key="wwtw_code"),
            "rates_lookup": LookupRowRule(json_key="nn_catchment"),
        }
    )
    sql = _lookup_row_sql("lookup_table", rules)
    assert "name = 'wwtw_lookup'" in sql
    assert "elem->>'wwtw_code' IS NULL" in sql
    assert "name = 'rates_lookup'" in sql
    assert "elem->>'nn_catchment' IS NULL" in sql
    assert sql.count("rule=key_not_null") == 2
    assert sql.count("rule=key_unique") == 2


def test_geometry_sql_checks_validity_srid_and_type():
    from app.data_sync.qc import _geometry_sql
    from app.data_sync.qc_rules import GeometryRule, TableRules

    rules = TableRules(
        geometry=GeometryRule(expected_type="Polygon", expected_srid=27700)
    )
    sql = _geometry_sql("nn_catchments", rules)
    assert "NOT ST_IsValid(geometry) AND NOT ST_IsValid(ST_MakeValid(geometry))" in sql
    assert "rule=geometry_valid" in sql
    assert "ST_SRID(geometry) NOT IN (0, 27700)" in sql
    assert "rule=geometry_srid" in sql
    assert "GeometryType(geometry) <> 'POLYGON'" in sql
    assert "rule=geometry_type" in sql


def test_geometry_sql_uses_multipolygon_expectation():
    from app.data_sync.qc import _geometry_sql
    from app.data_sync.qc_rules import GeometryRule, TableRules

    rules = TableRules(
        geometry=GeometryRule(expected_type="MultiPolygon", expected_srid=27700)
    )
    sql = _geometry_sql("lpa_boundaries", rules)
    assert "GeometryType(geometry) <> 'MULTIPOLYGON'" in sql


def test_coefficient_range_sql_checks_bounds_and_finiteness():
    from app.data_sync.qc import _coefficient_range_sql
    from app.data_sync.qc_rules import CoefficientRange, TableRules

    rules = TableRules(
        coefficient_ranges={
            "lu_curr_n_coeff": CoefficientRange(min=0, max=50),
            "n_resi_coeff": CoefficientRange(min=0, max=50),
        }
    )
    sql = _coefficient_range_sql("coefficient_layer", rules)
    assert sql.count("rule=coefficient_range") == 2
    assert "lu_curr_n_coeff < 0.0 OR lu_curr_n_coeff > 50.0" in sql
    assert "lu_curr_n_coeff = 'NaN'::float8" in sql
    assert "lu_curr_n_coeff = 'Infinity'::float8" in sql
    assert "lu_curr_n_coeff = '-Infinity'::float8" in sql
    assert "n_resi_coeff < 0.0 OR n_resi_coeff > 50.0" in sql


def test_referential_sql_both_sides_staged():
    from app.data_sync.qc import _referential_sql
    from app.data_sync.qc_rules import ReferentialCheck, ReferentialSide

    check = ReferentialCheck(
        name="coefficient_layer_nn_catchment",
        **{"from": {"table": "coefficient_layer", "column": "nn_catchment"}},
        to=ReferentialSide(table="nn_catchments", json_key="attributes.N2K_Site_N"),
    )
    sql = _referential_sql(check, staged_tables={"coefficient_layer", "nn_catchments"})
    assert "FROM pg_temp._ds_stage_coefficient_layer" in sql
    assert "SELECT nn_catchment AS v" in sql
    assert "FROM pg_temp._ds_stage_nn_catchments" in sql
    assert "attributes->>'N2K_Site_N'" in sql
    assert "rule=referential_coefficient_layer_nn_catchment" in sql


def test_referential_sql_falls_back_to_live_table_when_not_staged():
    from app.data_sync.qc import _referential_sql
    from app.data_sync.qc_rules import ReferentialCheck, ReferentialSide

    check = ReferentialCheck(
        name="coefficient_layer_subcatchment",
        **{"from": {"table": "coefficient_layer", "column": "subcatchment"}},
        to=ReferentialSide(table="subcatchments", json_key="attributes.OPCAT_NAME"),
    )
    sql = _referential_sql(check, staged_tables={"coefficient_layer"})
    assert "FROM public.subcatchments" in sql
    assert "FROM pg_temp._ds_stage_coefficient_layer" in sql


def test_referential_sql_lookup_row_source_and_numeric_coercion():
    from app.data_sync.qc import _referential_sql
    from app.data_sync.qc_rules import ReferentialCheck, ReferentialSide

    check = ReferentialCheck(
        name="wwtw_lookup_wwtw_code",
        **{
            "from": {
                "table": "lookup_table",
                "lookup_row": "wwtw_lookup",
                "json_key": "wwtw_code",
            }
        },
        to=ReferentialSide(table="wwtw_catchments", json_key="attributes.WwTw_ID"),
        numeric_coercion=True,
    )
    sql = _referential_sql(check, staged_tables={"lookup_table", "wwtw_catchments"})
    assert "jsonb_array_elements(data) elem" in sql
    assert "name = 'wwtw_lookup'" in sql
    assert "::numeric" in sql


def test_referential_sql_allow_null_from_guards_null_values():
    from app.data_sync.qc import _referential_sql
    from app.data_sync.qc_rules import ReferentialCheck, ReferentialSide

    check = ReferentialCheck(
        name="wwtw_lookup_subcatchment",
        **{
            "from": {
                "table": "lookup_table",
                "lookup_row": "wwtw_lookup",
                "json_key": "wwtw_subcatchment",
            }
        },
        to=ReferentialSide(table="subcatchments", json_key="attributes.OPCAT_NAME"),
        allow_null_from=True,
    )
    sql = _referential_sql(check, staged_tables={"lookup_table", "subcatchments"})
    assert "f.v IS NOT NULL AND NOT EXISTS" in sql


def test_build_qc_sql_wraps_rules_in_do_block_and_raises_on_failure():
    from pathlib import Path

    from app.data_sync.qc import build_qc_sql
    from app.data_sync.qc_rules import load_qc_rules

    rules = load_qc_rules()
    items = [
        ("nn_catchments", Path("dummy.gz")),
        ("coefficient_layer", Path("dummy2.gz")),
    ]
    sql = build_qc_sql(items, rules)

    assert sql.startswith("DO $qc$\n")
    assert sql.rstrip().endswith("$qc$;")
    assert "failures text[] := ARRAY[]::text[]" in sql
    assert "detail_count bigint" in sql
    assert "staged_count bigint" in sql
    assert "prev_count bigint" in sql
    assert "rule=row_count" in sql  # nn_catchments row-count rule present
    assert "rule=key_not_null" in sql  # nn_catchments JSON key rule present
    assert "rule=geometry_valid" in sql  # nn_catchments geometry rule present
    assert "rule=coefficient_range" in sql  # coefficient_layer rule present
    assert "IF array_length(failures, 1) > 0 THEN" in sql
    assert "RAISE EXCEPTION '%', array_to_string(failures, E'\\n');" in sql


def test_build_qc_sql_only_includes_referential_check_when_a_side_is_staged():
    from pathlib import Path

    from app.data_sync.qc import build_qc_sql
    from app.data_sync.qc_rules import load_qc_rules

    rules = load_qc_rules()
    items = [("gcn_ponds", Path("dummy.gz"))]  # no referential pairs involve gcn_ponds
    sql = build_qc_sql(items, rules)
    assert "rule=referential_" not in sql
    assert "rule=geometry_valid" in sql  # gcn_ponds geometry rule still runs


def test_build_qc_sql_skips_unrecognized_table():
    from pathlib import Path

    from app.data_sync.qc import build_qc_sql
    from app.data_sync.qc_rules import load_qc_rules

    rules = load_qc_rules()
    sql = build_qc_sql([("not_a_real_table", Path("x.gz"))], rules)
    assert "not_a_real_table" not in sql


def test_parse_qc_failures_extracts_lines_from_psql_error():
    from app.data_sync.qc import QcFailure, parse_qc_failures

    error = (
        "psql atomic restore failed: psql:<stdin>:12: ERROR:  "
        "table=coefficient_layer rule=row_count detail=staged row count 3 is below the 90% floor of previous 100 (floor=90)\n"
        "table=nn_catchments rule=key_unique detail=2 duplicate attributes.OID value(s)\n"
        "CONTEXT:  PL/pgSQL function inline_code_block line 45 at RAISE\n"
    )
    failures = parse_qc_failures(error)
    assert failures == [
        QcFailure(
            table="coefficient_layer",
            rule="row_count",
            detail="staged row count 3 is below the 90% floor of previous 100 (floor=90)",
        ),
        QcFailure(
            table="nn_catchments",
            rule="key_unique",
            detail="2 duplicate attributes.OID value(s)",
        ),
    ]


def test_parse_qc_failures_returns_empty_for_non_qc_error():
    from app.data_sync.qc import parse_qc_failures

    error = 'psql atomic restore failed: relation "lpa_boundaries" has no column "no_such_column"'
    assert parse_qc_failures(error) == []


def test_parse_qc_failures_handles_detail_containing_table_and_rule_like_text():
    from app.data_sync.qc import QcFailure, parse_qc_failures

    error = (
        "psql atomic restore failed: psql:<stdin>:9: ERROR:  "
        "table=coefficient_layer rule=coefficient_range detail=value for column "
        "table=x rule=y was rejected\n"
    )
    failures = parse_qc_failures(error)
    assert failures == [
        QcFailure(
            table="coefficient_layer",
            rule="coefficient_range",
            detail="value for column table=x rule=y was rejected",
        )
    ]


def test_parse_qc_failures_empty_string_returns_empty_list():
    from app.data_sync.qc import parse_qc_failures

    assert parse_qc_failures("") == []
