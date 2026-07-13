"""SQL generation for the staging-to-live QC gate (DM-1).

Builds one `DO $qc$ ... $qc$;` block that runs between the STAGE and PROMOTE
passes of `restore_all_atomic`, checking every DM-2-confirmed rule against every
staged table and accumulating failures into a PL/pgSQL array. A single
`RAISE EXCEPTION` at the end aborts the enclosing `psql --single-transaction`,
rolling back the whole manifest so bad data never reaches the live tables.
"""

import re
from pathlib import Path
from typing import NamedTuple

from app.data_sync.qc_rules import (
    QcRules,
    ReferentialCheck,
    ReferentialSide,
    TableRules,
)
from app.data_sync.restore import staging_name


def _row_count_sql(table: str, rules: TableRules, floor_pct: float) -> str:
    """Rule 2: staged row count is non-zero and >= floor_pct% of the live
    table's current (pre-promotion) row count. `floor_pct` is the table's
    override if set, else the global default.

    The `staged_count = 0` hard fail below is unconditional per the DM-2
    sign-off, which applies it to all 10 reference tables; there is no
    per-table flag to disable it.
    """
    stage = staging_name(table)
    pct = (
        rules.row_count_floor_pct
        if rules.row_count_floor_pct is not None
        else floor_pct
    )
    ratio = pct / 100
    return (
        f"SELECT COUNT(*) INTO staged_count FROM pg_temp.{stage};\n"  # noqa: S608
        f"SELECT COUNT(*) INTO prev_count FROM public.{table} "
        f"WHERE version = (SELECT MAX(version) FROM public.{table});\n"
        "IF staged_count = 0 THEN\n"
        f"  failures := array_append(failures, "
        f"'table={table} rule=row_count detail=staged row count is 0');\n"
        f"ELSIF prev_count > 0 AND staged_count < CEIL(prev_count * {ratio}) THEN\n"
        "  failures := array_append(failures, format("
        f"'table={table} rule=row_count detail=staged row count %s is below "
        f"the {pct}%% floor of previous %s (floor=%s)', "
        "staged_count, prev_count, CEIL(prev_count * "
        f"{ratio})));\n"
        "END IF;\n"
    )


def _column_key_sql(table: str, rules: TableRules) -> str:
    """Rules 3 & 8 for a plain-column business key: no NULLs, unique within the
    staged set, plus any additional required-non-null columns.
    """
    stage = staging_name(table)
    key = rules.key
    cols = ", ".join(key.columns)
    null_predicate = " OR ".join(f"{c} IS NULL" for c in key.columns)
    sql = (
        f"SELECT COUNT(*) INTO detail_count FROM pg_temp.{stage} "  # noqa: S608
        f"WHERE {null_predicate};\n"
        "IF detail_count > 0 THEN\n"
        "  failures := array_append(failures, format("
        f"'table={table} rule=key_not_null detail=%s row(s) with NULL key "
        f"({cols})', detail_count));\n"
        "END IF;\n"
    )
    if key.unique:
        sql += (
            f"SELECT COUNT(*) INTO detail_count FROM (SELECT {cols} "  # noqa: S608
            f"FROM pg_temp.{stage} GROUP BY {cols} HAVING COUNT(*) > 1) dup;\n"
            "IF detail_count > 0 THEN\n"
            "  failures := array_append(failures, format("
            f"'table={table} rule=key_unique detail=%s duplicate key value(s) "
            f"({cols})', detail_count));\n"
            "END IF;\n"
        )
    for extra in rules.non_null_columns:
        sql += (
            f"SELECT COUNT(*) INTO detail_count FROM pg_temp.{stage} "  # noqa: S608
            f"WHERE {extra} IS NULL;\n"
            "IF detail_count > 0 THEN\n"
            "  failures := array_append(failures, format("
            f"'table={table} rule=non_null detail=%s row(s) with NULL {extra}', "
            "detail_count));\n"
            "END IF;\n"
        )
    return sql


def _json_path_expr(path: str) -> str:
    """Turn a declared `column.key` path (e.g. "attributes.OID") into a JSONB
    text-extraction expression (e.g. "attributes->>'OID'")."""
    col, _, key = path.partition(".")
    return f"{col}->>'{key}'"


def _json_key_sql(table: str, rules: TableRules) -> str:
    """Rules 3 & 8 for a JSONB-attribute business key, plus any additional
    required-non-null JSON columns and any allowed-value enum constraints.

    Only single-column JSON keys are supported; composite JSON keys are not
    implemented (unlike `_column_key_sql`, which does support composite keys).
    """
    stage = staging_name(table)
    key = rules.key
    if len(key.columns) != 1:
        msg = (
            f"table={table!r}: JSON-sourced keys support exactly one column, "
            f"got {key.columns!r} (composite JSON keys are not implemented)"
        )
        raise ValueError(msg)
    path = key.columns[0]
    expr = _json_path_expr(path)
    sql = (
        f"SELECT COUNT(*) INTO detail_count FROM pg_temp.{stage} "  # noqa: S608
        f"WHERE {expr} IS NULL;\n"
        "IF detail_count > 0 THEN\n"
        "  failures := array_append(failures, format("
        f"'table={table} rule=key_not_null detail=%s row(s) with NULL {path}', "
        "detail_count));\n"
        "END IF;\n"
    )
    if key.unique:
        sql += (
            f"SELECT COUNT(*) INTO detail_count FROM (SELECT {expr} AS k "  # noqa: S608
            f"FROM pg_temp.{stage} GROUP BY {expr} HAVING COUNT(*) > 1) dup;\n"
            "IF detail_count > 0 THEN\n"
            "  failures := array_append(failures, format("
            f"'table={table} rule=key_unique detail=%s duplicate {path} "
            "value(s)', detail_count));\n"
            "END IF;\n"
        )
    for extra in rules.non_null_json_columns:
        extra_expr = _json_path_expr(extra)
        sql += (
            f"SELECT COUNT(*) INTO detail_count FROM pg_temp.{stage} "  # noqa: S608
            f"WHERE {extra_expr} IS NULL;\n"
            "IF detail_count > 0 THEN\n"
            "  failures := array_append(failures, format("
            f"'table={table} rule=non_null detail=%s row(s) with NULL {extra}', "
            "detail_count));\n"
            "END IF;\n"
        )
    for path2, allowed in rules.allowed_values.items():
        expr2 = _json_path_expr(path2)
        values = ", ".join(f"'{v}'" for v in allowed)
        values_display = ", ".join(f"''{v}''" for v in allowed)
        sql += (
            f"SELECT COUNT(*) INTO detail_count FROM pg_temp.{stage} "  # noqa: S608
            f"WHERE {expr2} NOT IN ({values});\n"
            "IF detail_count > 0 THEN\n"
            "  failures := array_append(failures, format("
            f"'table={table} rule=allowed_values detail=%s row(s) with {path2} "
            f"outside {{{values_display}}}', detail_count));\n"
            "END IF;\n"
        )
    return sql


def _lookup_row_sql(table: str, rules: TableRules) -> str:
    """Rules 3 & 8 for `lookup_table` rows whose business key lives inside the
    JSONB `data` array (identified by `name`), e.g. `wwtw_lookup`/`rates_lookup`.
    """
    stage = staging_name(table)
    sql = ""
    for row_name, row_rule in rules.lookup_rows.items():
        jk = row_rule.json_key
        sql += (
            f"SELECT COUNT(*) INTO detail_count FROM pg_temp.{stage}, "  # noqa: S608
            f"jsonb_array_elements(data) elem WHERE name = '{row_name}' "
            f"AND elem->>'{jk}' IS NULL;\n"
            "IF detail_count > 0 THEN\n"
            "  failures := array_append(failures, format("
            f"'table={table} rule=key_not_null detail=%s row(s) in {row_name} "
            f"with NULL {jk}', detail_count));\n"
            "END IF;\n"
            "SELECT COUNT(*) INTO detail_count FROM (SELECT elem->>'"
            f"{jk}' AS k FROM pg_temp.{stage}, jsonb_array_elements(data) elem "
            f"WHERE name = '{row_name}' GROUP BY k HAVING COUNT(*) > 1) dup;\n"
            "IF detail_count > 0 THEN\n"
            "  failures := array_append(failures, format("
            f"'table={table} rule=key_unique detail=%s duplicate {jk} value(s) "
            f"in {row_name}', detail_count));\n"
            "END IF;\n"
        )
    return sql


def _geometry_sql(table: str, rules: TableRules) -> str:
    """Rules 4-6: geometry validity (with ST_MakeValid repair check), SRID
    (0 is treated as unset-and-therefore-27700 per DM-2), and declared type.
    """
    stage = staging_name(table)
    geom = rules.geometry
    expected_type = geom.expected_type.upper()
    return (
        f"SELECT COUNT(*) INTO detail_count FROM pg_temp.{stage} "  # noqa: S608
        "WHERE NOT ST_IsValid(geometry) AND NOT ST_IsValid(ST_MakeValid(geometry));\n"
        "IF detail_count > 0 THEN\n"
        "  failures := array_append(failures, format("
        f"'table={table} rule=geometry_valid detail=%s row(s) with invalid, "
        "unrepairable geometry', detail_count));\n"
        "END IF;\n"
        f"SELECT COUNT(*) INTO detail_count FROM pg_temp.{stage} "
        f"WHERE ST_SRID(geometry) NOT IN (0, {geom.expected_srid});\n"
        "IF detail_count > 0 THEN\n"
        "  failures := array_append(failures, format("
        f"'table={table} rule=geometry_srid detail=%s row(s) with SRID other "
        f"than {geom.expected_srid}', detail_count));\n"
        "END IF;\n"
        f"SELECT COUNT(*) INTO detail_count FROM pg_temp.{stage} "
        f"WHERE GeometryType(geometry) <> '{expected_type}';\n"
        "IF detail_count > 0 THEN\n"
        "  failures := array_append(failures, format("
        f"'table={table} rule=geometry_type detail=%s row(s) with geometry "
        f"type other than {geom.expected_type}', detail_count));\n"
        "END IF;\n"
    )


def _coefficient_range_sql(table: str, rules: TableRules) -> str:
    """Rule 7: each declared coefficient column, when non-NULL, must be a
    finite number within its confirmed hard bounds.
    """
    stage = staging_name(table)
    sql = ""
    for col, rng in rules.coefficient_ranges.items():
        sql += (
            f"SELECT COUNT(*) INTO detail_count FROM pg_temp.{stage} "  # noqa: S608
            f"WHERE {col} IS NOT NULL AND ({col} < {rng.min} OR {col} > {rng.max} "
            f"OR {col} = 'NaN'::float8 OR {col} = 'Infinity'::float8 "
            f"OR {col} = '-Infinity'::float8);\n"
            "IF detail_count > 0 THEN\n"
            "  failures := array_append(failures, format("
            f"'table={table} rule=coefficient_range detail=%s row(s) with "
            f"{col} outside [{rng.min}, {rng.max}]', detail_count));\n"
            "END IF;\n"
        )
    return sql


def _referential_source(side: ReferentialSide, staged_tables: set[str]) -> str:
    if side.table in staged_tables:
        return f"pg_temp.{staging_name(side.table)}"
    return f"public.{side.table}"


def _referential_side_select(side: ReferentialSide, source: str, alias: str) -> str:
    if side.lookup_row is not None:
        return (
            f"SELECT elem->>'{side.json_key}' AS v FROM {source}, "  # noqa: S608
            f"jsonb_array_elements(data) elem WHERE name = '{side.lookup_row}'"
        )
    if side.json_key is not None:
        return (
            f"SELECT {'DISTINCT ' if alias == 'to_' else ''}"  # noqa: S608
            f"{_json_path_expr(side.json_key)} AS v FROM {source}"
        )
    return f"SELECT {'DISTINCT ' if alias == 'to_' else ''}{side.column} AS v FROM {source}"  # noqa: S608


def _referential_sql(check: ReferentialCheck, staged_tables: set[str]) -> str:
    """Rule 9: every value on the `from` side of a confirmed referential pair
    must exist on the `to` side, after any declared numeric coercion or
    null-guarding.
    """
    from_source = _referential_source(check.from_, staged_tables)
    to_source = _referential_source(check.to, staged_tables)
    from_select = _referential_side_select(check.from_, from_source, "from_")
    to_select = _referential_side_select(check.to, to_source, "to_")

    cast = "::numeric" if check.numeric_coercion else ""
    null_guard = "f.v IS NOT NULL AND " if check.allow_null_from else ""
    return (
        f"SELECT COUNT(*) INTO detail_count FROM ({from_select}) f "  # noqa: S608
        f"WHERE {null_guard}NOT EXISTS "
        f"(SELECT 1 FROM ({to_select}) t WHERE t.v{cast} = f.v{cast});\n"
        "IF detail_count > 0 THEN\n"
        "  failures := array_append(failures, format("
        f"'table={check.from_.table} rule=referential_{check.name} detail=%s "
        f"row(s) failing referential check {check.name}', detail_count));\n"
        "END IF;\n"
    )


def _table_parts(table: str, rules: TableRules, floor_pct: float) -> list[str]:
    """Every applicable per-table rule for one staged table, in check order."""
    parts = [_row_count_sql(table, rules, floor_pct)]
    if rules.key is not None:
        if rules.key.source == "column":
            parts.append(_column_key_sql(table, rules))
        else:
            parts.append(_json_key_sql(table, rules))
    if rules.lookup_rows:
        parts.append(_lookup_row_sql(table, rules))
    if rules.geometry is not None:
        parts.append(_geometry_sql(table, rules))
    if rules.coefficient_ranges:
        parts.append(_coefficient_range_sql(table, rules))
    return parts


def _referential_parts(
    checks: list[ReferentialCheck], staged_tables: set[str]
) -> list[str]:
    """Rule 9 for every referential check touching a staged table, de-duplicated
    by check name so a check shared across tables is emitted only once.
    """
    seen_checks: set[str] = set()
    parts = []
    for check in checks:
        if check.name in seen_checks:
            continue
        if check.from_.table in staged_tables or check.to.table in staged_tables:
            parts.append(_referential_sql(check, staged_tables))
            seen_checks.add(check.name)
    return parts


def build_qc_sql(items: list[tuple[str, Path]], rules: QcRules) -> str:
    """Build the full `DO $qc$ ... $qc$;` block checking every applicable rule
    against every table in `items`. Raises (via the generated SQL) once, with
    every failing rule across every table joined by newlines, if any rule
    fails — the caller's enclosing transaction then rolls back atomically.
    """
    staged_tables = {table for table, _ in items}
    parts = [
        "DO $qc$\n"
        "DECLARE\n"
        "  failures text[] := ARRAY[]::text[];\n"
        "  detail_count bigint;\n"
        "  staged_count bigint;\n"
        "  prev_count bigint;\n"
        "BEGIN\n"
    ]
    for table in staged_tables:
        table_rules = rules.tables.get(table)
        if table_rules is not None:
            parts.extend(_table_parts(table, table_rules, rules.row_count_floor_pct))
    parts.extend(_referential_parts(rules.referential_checks, staged_tables))
    parts.append(
        "IF array_length(failures, 1) > 0 THEN\n"
        "  RAISE EXCEPTION '%', array_to_string(failures, E'\\n');\n"
        "END IF;\n"
        "END;\n"
        "$qc$;\n"
    )
    return "".join(parts)


class QcFailure(NamedTuple):
    """One parsed `table=X rule=Y detail=...` line from a QC failure message."""

    table: str
    rule: str
    detail: str


_FAILURE_LINE_RE = re.compile(
    r"table=(?P<table>\S+) rule=(?P<rule>\S+) detail=(?P<detail>.*)"
)


def parse_qc_failures(error: str) -> list[QcFailure]:
    """Extract every `table=X rule=Y detail=...` line from a
    `restore_all_atomic` error message. Returns `[]` for a non-QC error (bad
    gzip, broken COPY, connection failure) so unrelated failures are untouched.
    """
    failures = []
    for line in error.splitlines():
        match = _FAILURE_LINE_RE.search(line)
        if match:
            failures.append(
                QcFailure(
                    table=match.group("table"),
                    rule=match.group("rule"),
                    detail=match.group("detail").strip(),
                )
            )
    return failures
