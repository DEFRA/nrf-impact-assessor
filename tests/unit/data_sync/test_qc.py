import pytest

from app.data_sync.qc import _RATIO_MIN_PREV_COUNT, _row_count_sql
from app.data_sync.qc_rules import TableRules


def test_row_count_sql_checks_zero_and_floor():
    rules = TableRules()
    sql = _row_count_sql("nn_catchments", rules, floor_pct=90, active_version=None)
    assert "FROM pg_temp._ds_stage_nn_catchments" in sql
    assert "INTO staged_count" in sql
    assert "FROM public.nn_catchments" in sql
    assert "INTO prev_count" in sql
    assert "staged_count = 0" in sql
    assert "rule=row_count" in sql
    assert "staged_count < CEIL(prev_count * 0.9)" in sql


def test_row_count_sql_uses_per_table_override():
    rules = TableRules(row_count_floor_pct=50)
    sql = _row_count_sql("gcn_ponds", rules, floor_pct=90, active_version=None)
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
    # non_null_json_columns is emitted by _non_null_json_sql, not here, so that
    # a table declaring one without a key still gets the check.
    assert "attributes->>'N2K_Site_N' IS NULL" not in sql


def test_json_key_sql_non_unique_skips_uniqueness_check():
    from app.data_sync.qc import _json_key_sql
    from app.data_sync.qc_rules import KeyRule, TableRules

    rules = TableRules(
        key=KeyRule(columns=["attributes.NAME"], source="json", unique=False)
    )
    sql = _json_key_sql("lpa_boundaries", rules)
    assert "rule=key_unique" not in sql


def test_json_key_sql_omits_allowed_values():
    """allowed_values is emitted by _allowed_values_sql, not here.

    Keeping it out of the key SQL is what lets a keyless table declare enum
    constraints and still have them checked.
    """
    from app.data_sync.qc import _json_key_sql
    from app.data_sync.qc_rules import KeyRule, TableRules

    rules = TableRules(
        key=KeyRule(columns=["attributes.RZ"], source="json", unique=False),
        allowed_values={"attributes.RZ": ["Red", "Amber", "Green"]},
    )
    sql = _json_key_sql("gcn_risk_zones", rules)
    assert "rule=allowed_values" not in sql


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
        geometry=GeometryRule(expected_types=["Polygon"], expected_srid=27700)
    )
    sql = _geometry_sql("nn_catchments", rules)
    assert "NOT ST_IsValid(geometry) AND NOT ST_IsValid(ST_MakeValid(geometry))" in sql
    assert "rule=geometry_valid" in sql
    assert "ST_SRID(geometry) NOT IN (0, 27700)" in sql
    assert "rule=geometry_srid" in sql
    assert "GeometryType(geometry) NOT IN ('POLYGON')" in sql
    assert "rule=geometry_type" in sql


def test_geometry_sql_uses_multipolygon_expectation():
    from app.data_sync.qc import _geometry_sql
    from app.data_sync.qc_rules import GeometryRule, TableRules

    rules = TableRules(
        geometry=GeometryRule(expected_types=["MultiPolygon"], expected_srid=27700)
    )
    sql = _geometry_sql("lpa_boundaries", rules)
    assert "GeometryType(geometry) NOT IN ('MULTIPOLYGON')" in sql


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


def test_referential_source_staged_uses_stage():
    from app.data_sync.qc import _referential_source
    from app.data_sync.qc_rules import ReferentialSide

    side = ReferentialSide(table="nn_catchments", column="id")
    src = _referential_source(side, staged_tables={"nn_catchments"}, active_versions={})
    assert src == "pg_temp._ds_stage_nn_catchments"


def test_referential_source_unstaged_filters_to_active_version():
    from app.data_sync.qc import _referential_source
    from app.data_sync.qc_rules import ReferentialSide

    side = ReferentialSide(table="nn_catchments", column="id")
    src = _referential_source(
        side, staged_tables=set(), active_versions={"nn_catchments": 7}
    )
    assert src == "(SELECT * FROM public.nn_catchments WHERE version = 7)"


def test_referential_sql_both_sides_staged():
    from app.data_sync.qc import _referential_sql
    from app.data_sync.qc_rules import ReferentialCheck, ReferentialSide

    check = ReferentialCheck(
        name="coefficient_layer_nn_catchment",
        **{"from": {"table": "coefficient_layer", "column": "nn_catchment"}},
        to=ReferentialSide(table="nn_catchments", json_key="attributes.N2K_Site_N"),
    )
    sql = _referential_sql(
        check,
        staged_tables={"coefficient_layer", "nn_catchments"},
        active_versions={},
    )
    assert "FROM pg_temp._ds_stage_coefficient_layer" in sql
    assert "SELECT nn_catchment AS v" in sql
    assert "FROM pg_temp._ds_stage_nn_catchments" in sql
    assert "attributes->>'N2K_Site_N'" in sql
    assert "rule=referential_coefficient_layer_nn_catchment" in sql


def test_referential_sql_pins_unstaged_side_to_active_version():
    from app.data_sync.qc import _referential_sql
    from app.data_sync.qc_rules import ReferentialCheck, ReferentialSide

    check = ReferentialCheck(
        name="coefficient_layer_subcatchment",
        **{"from": {"table": "coefficient_layer", "column": "subcatchment"}},
        to=ReferentialSide(table="subcatchments", json_key="attributes.OPCAT_NAME"),
    )
    sql = _referential_sql(
        check,
        staged_tables={"coefficient_layer"},
        active_versions={"subcatchments": 4},
    )
    assert "(SELECT * FROM public.subcatchments WHERE version = 4)" in sql
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
    sql = _referential_sql(
        check,
        staged_tables={"lookup_table", "wwtw_catchments"},
        active_versions={},
    )
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
    sql = _referential_sql(
        check,
        staged_tables={"lookup_table", "subcatchments"},
        active_versions={},
    )
    assert "f.v IS NOT NULL AND NOT EXISTS" in sql


def _qc_item(table: str):
    """A RestoreItem for QC tests; build_qc_sql only reads `.table`."""
    from pathlib import Path

    from app.data_sync.restore import RestoreItem

    return RestoreItem(
        table=table,
        dumps=[Path("dummy.gz")],
        s3_key="k/1",
        etag="etag1",
        data_version="v1",
    )


def test_build_qc_sql_wraps_rules_in_do_block_and_raises_on_failure():
    from app.data_sync.qc import build_qc_sql
    from app.data_sync.qc_rules import load_qc_rules

    rules = load_qc_rules()
    items = [_qc_item("nn_catchments"), _qc_item("coefficient_layer")]
    # lookup_table and subcatchments are referenced but unstaged, so their
    # active versions must be supplied for the referential reads.
    sql = build_qc_sql(
        items, rules, active_versions={"lookup_table": 1, "subcatchments": 1}
    )

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
    from app.data_sync.qc import build_qc_sql
    from app.data_sync.qc_rules import load_qc_rules

    rules = load_qc_rules()
    items = [_qc_item("gcn_ponds")]  # no referential pairs involve gcn_ponds
    sql = build_qc_sql(items, rules)
    assert "rule=referential_" not in sql
    assert "rule=geometry_valid" in sql  # gcn_ponds geometry rule still runs


def test_build_qc_sql_skips_unrecognized_table():
    from app.data_sync.qc import build_qc_sql
    from app.data_sync.qc_rules import load_qc_rules

    rules = load_qc_rules()
    sql = build_qc_sql([_qc_item("not_a_real_table")], rules)
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


def test_row_count_pins_previous_count_to_the_active_version():
    """After a rollback, MAX(version) is the rolled-back-FROM version, which is
    no longer live. The floor must be measured against the active version, the
    same pin the referential checks use — otherwise a rollback away from a bad
    small load leaves the floor computed off that bad load's row count.
    """
    from app.data_sync.qc import build_qc_sql
    from app.data_sync.qc_rules import load_qc_rules

    rules = load_qc_rules()
    sql = build_qc_sql(
        [_qc_item("nn_catchments")],
        rules,
        active_versions={
            "nn_catchments": 1,
            "lookup_table": 1,
            "coefficient_layer": 1,
            "subcatchments": 1,
        },
    )
    assert (
        "SELECT COUNT(*) INTO prev_count FROM public.nn_catchments WHERE version = 1;"
        in sql
    )
    assert (
        "INTO prev_count FROM public.nn_catchments WHERE version = (SELECT MAX"
        not in sql
    )


def test_row_count_falls_back_to_max_version_without_an_active_version():
    """A table with no supplied active version (e.g. its first ever load) keeps
    the previous MAX(version) behaviour.
    """
    from app.data_sync.qc import build_qc_sql
    from app.data_sync.qc_rules import load_qc_rules

    rules = load_qc_rules()
    sql = build_qc_sql([_qc_item("gcn_ponds")], rules)
    assert (
        "SELECT COUNT(*) INTO prev_count FROM public.gcn_ponds "
        "WHERE version = (SELECT MAX(version) FROM public.gcn_ponds);" in sql
    )


def test_ratio_is_skipped_below_the_small_table_threshold():
    """The proportional floor only applies once the live table is big enough.

    At single-digit row counts a percentage cannot express anything useful: at
    3 rows the 90% default gives CEIL(3 * 0.9) = 3, so it blocks the loss of
    even one row, and any pct above ~34% still blocks a real 3 -> 1
    consolidation — which is what failed a legitimate edp_boundary_layer
    reload.
    """
    sql = _row_count_sql(
        "edp_boundary_layer", TableRules(), floor_pct=90, active_version=None
    )
    assert f"ELSIF prev_count >= {_RATIO_MIN_PREV_COUNT} " in sql
    # The zero-row hard fail is unconditional (DM-2) and still covers small tables.
    assert "staged_count = 0" in sql


def test_no_table_opts_out_of_the_row_count_floor():
    """The small-table skip is a global rule, so no per-table override is
    needed. A `row_count_floor_pct` here would disable the ratio permanently,
    including once the table grew past the threshold.
    """
    from app.data_sync.qc_rules import load_qc_rules

    rules = load_qc_rules()
    assert rules.row_count_floor_pct == 90
    overridden = {
        name: r.row_count_floor_pct
        for name, r in rules.tables.items()
        if r.row_count_floor_pct is not None
    }
    assert overridden == {}


# ---------------------------------------------------------------------------
# non_null_json_columns / allowed_values are independent of the key rule
# ---------------------------------------------------------------------------


def test_non_null_json_sql_checks_each_column():
    from app.data_sync.qc import _non_null_json_sql
    from app.data_sync.qc_rules import TableRules

    rules = TableRules(non_null_json_columns=["attributes.site_name"])
    sql = _non_null_json_sql("edp_excluded_areas", rules)

    assert "FROM pg_temp._ds_stage_edp_excluded_areas" in sql
    assert "attributes->>'site_name' IS NULL" in sql
    assert "rule=non_null" in sql


def test_allowed_values_sql_checks_each_column():
    from app.data_sync.qc import _allowed_values_sql
    from app.data_sync.qc_rules import TableRules

    rules = TableRules(allowed_values={"attributes.RZ": ["Red", "Amber", "Green"]})
    sql = _allowed_values_sql("gcn_risk_zones", rules)

    assert "attributes->>'RZ' NOT IN ('Red', 'Amber', 'Green')" in sql
    assert "rule=allowed_values" in sql
    assert "outside {''Red'', ''Amber'', ''Green''}" in sql


def test_keyless_table_still_gets_its_non_null_checks():
    """A non-null rule must not depend on the table also declaring a key.

    These checks used to be emitted only from inside _json_key_sql, so a table
    with non_null_json_columns and no `key` had its rule silently ignored.
    """
    from app.data_sync.qc import _table_parts
    from app.data_sync.qc_rules import TableRules

    rules = TableRules(non_null_json_columns=["attributes.site_name"])
    sql = "".join(
        _table_parts("edp_excluded_areas", rules, floor_pct=90, active_version=None)
    )

    assert rules.key is None
    assert "attributes->>'site_name' IS NULL" in sql
    assert "rule=non_null" in sql


def test_keyless_table_still_gets_its_allowed_values_checks():
    from app.data_sync.qc import _table_parts
    from app.data_sync.qc_rules import TableRules

    rules = TableRules(allowed_values={"attributes.RZ": ["Red", "Amber"]})
    sql = "".join(
        _table_parts("gcn_risk_zones", rules, floor_pct=90, active_version=None)
    )

    assert rules.key is None
    assert "rule=allowed_values" in sql


def test_non_null_checks_are_not_duplicated_when_a_key_exists():
    """nn_catchments has both a key and a non-null column: emit the check once."""
    from app.data_sync.qc import _table_parts
    from app.data_sync.qc_rules import KeyRule, TableRules

    rules = TableRules(
        key=KeyRule(columns=["attributes.OID"], source="json", unique=True),
        non_null_json_columns=["attributes.N2K_Site_N"],
    )
    sql = "".join(
        _table_parts("nn_catchments", rules, floor_pct=90, active_version=None)
    )

    assert sql.count("attributes->>'N2K_Site_N' IS NULL") == 1


@pytest.mark.parametrize(
    ("table", "column"),
    [
        ("edp_excluded_areas", "site_name"),
        ("edp_boundary_layer", "EDP_Name"),
    ],
)
def test_real_config_enforces_edp_layer_names(table, column):
    """Both EDP layers declare a non-null name rule and neither declares a key.

    The check-boundary exclusion gate depends on edp_excluded_areas.site_name
    being present, so this asserts against the shipped qc_rules.yaml rather than
    a synthetic TableRules.
    """
    from app.data_sync.qc import _table_parts
    from app.data_sync.qc_rules import load_qc_rules

    rules = load_qc_rules().tables[table]
    sql = "".join(_table_parts(table, rules, floor_pct=90, active_version=None))

    assert f"attributes->>'{column}' IS NULL" in sql
    assert "rule=non_null" in sql


# ---------------------------------------------------------------------------
# non_blank_columns: table columns that must be present and not whitespace
# ---------------------------------------------------------------------------


def test_non_blank_columns_sql_rejects_null_and_whitespace():
    from app.data_sync.qc import _non_blank_columns_sql
    from app.data_sync.qc_rules import TableRules

    rules = TableRules(non_blank_columns=["name"])
    sql = _non_blank_columns_sql("edp_excluded_areas", rules)

    assert "FROM pg_temp._ds_stage_edp_excluded_areas" in sql
    # COALESCE folds NULL into the blank check so one predicate covers both.
    assert "btrim(COALESCE(name, '')) = ''" in sql
    assert "rule=non_blank" in sql


def test_table_parts_includes_non_blank_columns():
    from app.data_sync.qc import _table_parts
    from app.data_sync.qc_rules import TableRules

    rules = TableRules(non_blank_columns=["name"])
    sql = "".join(
        _table_parts("edp_excluded_areas", rules, floor_pct=90, active_version=None)
    )

    assert "rule=non_blank" in sql


def test_real_config_requires_non_blank_excluded_area_name():
    """The exclusion gate reads the `name` column, not attributes.site_name.

    A row can satisfy the attributes.site_name non-null rule and still have a
    blank `name`, so the column itself needs its own rule.
    """
    from app.data_sync.qc import _table_parts
    from app.data_sync.qc_rules import load_qc_rules

    rules = load_qc_rules().tables["edp_excluded_areas"]
    sql = "".join(
        _table_parts("edp_excluded_areas", rules, floor_pct=90, active_version=None)
    )

    assert rules.non_blank_columns == ["name"]
    assert "btrim(COALESCE(name, '')) = ''" in sql
