"""Versioned, whole-manifest restore of gzipped data-only pg_dumps.

Each dump is `COPY ... FROM stdin`. All tables in a run stream into one
`psql --single-transaction` process. Per table we: create a TEMP staging table
shaped like the live table, redirect the dump's COPY into that staging table,
then stamp a fresh id + `version = MAX(version)+1` and `INSERT ... SELECT *`
into the live table. Because the whole batch shares one transaction, readers
keep seeing the prior version of every table until the final COMMIT, then flip
to the new version together; any error rolls back all tables.

This avoids the ACCESS EXCLUSIVE lock of TRUNCATE and needs no table ownership
(only INSERT, plus the database-default TEMPORARY privilege). Superseded
versions are removed by a best-effort post-commit cleanup in the service layer
(see app/data_sync/service.py). Indexes are defined once by Liquibase and
persist; index maintenance happens inline on INSERT (see NRF2-694 review for the
SECURITY DEFINER index-reset fallback if load time ever becomes a problem).
"""

import gzip
import logging
import os
import subprocess
import time
from pathlib import Path
from typing import IO

from app.config import DatabaseSettings
from app.data_sync.qc_rules import QcRules
from app.repositories.repository import _assert_safe_identifier

logger = logging.getLogger(__name__)


_STAGE_PREFIX = "_ds_stage_"


def staging_name(table: str) -> str:
    """Return the temp staging table name for `table` (validated)."""
    _assert_safe_identifier(table, "table")
    return f"{_STAGE_PREFIX}{table}"


def pre_sql(table: str) -> str:
    """SQL emitted before a table's COPY data: create the temp staging table.

    Columns only (no indexes) for a fast staging load. CREATE TEMP always
    targets pg_temp regardless of search_path. No BEGIN/COMMIT — the outer
    transaction is supplied by `psql --single-transaction`.
    """
    stage = staging_name(table)
    return f"CREATE TEMP TABLE {stage} (LIKE public.{table});\n"


def post_sql(table: str) -> str:
    """SQL emitted after a table's COPY data: stamp a fresh id + new version,
    load the live table from staging, then drop staging.

    `id` is regenerated (no FK references these ids) to avoid PK collisions with
    the rows already present; `version` is MAX(version)+1 computed once against
    the pre-insert snapshot. `LIKE` preserves column order, so `SELECT *` aligns.
    """
    stage = staging_name(table)
    # noqa justified: identifiers validated by staging_name/_assert_safe_identifier
    sql = (
        f"UPDATE pg_temp.{stage} SET id = gen_random_uuid(), "  # noqa: S608
        f"version = (SELECT COALESCE(MAX(version),0)+1 FROM public.{table});\n"
        f"INSERT INTO public.{table} SELECT * FROM pg_temp.{stage};\n"
        f"DROP TABLE pg_temp.{stage};\n"
    )
    return sql


def old_version_cleanup_sql(table: str) -> str:
    """SQL that deletes every superseded version, keeping only the latest."""
    _assert_safe_identifier(table, "table")
    # noqa justified: identifier validated by _assert_safe_identifier
    sql = (
        f"DELETE FROM public.{table} "  # noqa: S608
        f"WHERE version < (SELECT MAX(version) FROM public.{table});"
    )
    return sql


def build_psql_env(settings: DatabaseSettings, region: str) -> dict[str, str]:
    """Build PG* environment for a psql subprocess from DatabaseSettings."""
    env = dict(os.environ)
    env.update(
        PGHOST=settings.host,
        PGPORT=str(settings.port),
        PGDATABASE=settings.database,
        PGUSER=settings.user,
    )
    if settings.iam_authentication:
        from app.repositories.engine import _get_iam_auth_token

        env["PGPASSWORD"] = _get_iam_auth_token(settings, region)
        env["PGSSLMODE"] = settings.ssl_mode
    elif settings.local_password:
        env["PGPASSWORD"] = settings.local_password
    return env


_GZIP_MAGIC = b"\x1f\x8b"


def assert_gzip(table: str, dump_path: Path) -> None:
    """Fail fast with a clear error if a dump is not gzip-compressed.

    The dumps are streamed through `gzip.open` during restore; checking the
    magic bytes up front turns an opaque mid-transaction decompression error
    into an actionable message naming the offending table.
    """
    with dump_path.open("rb") as f:
        magic = f.read(2)
    if magic != _GZIP_MAGIC:
        msg = (
            f"dump for table {table!r} is not gzip-compressed "
            f"(expected gzip magic {_GZIP_MAGIC!r}, got {magic!r}); the S3 object "
            "must be a gzipped data-only pg_dump"
        )
        raise ValueError(msg)


def _rewrite_copy_line(line: bytes, table: str, stage: str) -> bytes:
    """Redirect a dump's `COPY public.<table> ...` header to the temp staging
    table. Lines that don't start with the exact header prefix are returned
    unchanged, so data rows containing the table name are never touched.
    """
    prefix = f"COPY public.{table} ".encode()
    if line.startswith(prefix):
        return f"COPY pg_temp.{stage} ".encode() + line[len(prefix) :]
    return line


def _stream_dump_to_staging(
    stdin: IO[bytes], dump_path: Path, table: str, stage: str
) -> None:
    """Stream a gzipped data-only dump into psql, redirecting its single COPY
    header to `pg_temp.<stage>`. The (small) preamble is read line-by-line until
    the header; the (large) data body is then streamed in 1 MiB chunks.
    """
    prefix = f"COPY public.{table} ".encode()
    with gzip.open(dump_path, "rb") as gz:
        found = False
        for line in gz:
            if line.startswith(prefix):
                stdin.write(_rewrite_copy_line(line, table, stage))
                found = True
                break
            stdin.write(line)
        if not found:
            msg = f"no COPY header for table {table!r} found in dump {dump_path}"
            raise ValueError(msg)
        for chunk in iter(lambda: gz.read(1024 * 1024), b""):
            stdin.write(chunk)


def restore_all_atomic(
    settings: DatabaseSettings,
    region: str,
    items: list[tuple[str, Path]],
    qc_rules: QcRules | None = None,
) -> None:
    """Load every (table, dump) in a single psql transaction. All-or-nothing.

    Validation for all tables happens up front (via staging_name), before psql
    is spawned, so an unsafe name aborts the whole batch before any subprocess
    side effect. `--single-transaction` + `ON_ERROR_STOP=1` make the batch
    atomic: the first error rolls back every table.

    When `qc_rules` is supplied, a generated QC `DO` block (see
    `app.data_sync.qc.build_qc_sql`) runs after every table has staged and
    before any table promotes, so a QC failure rolls back the whole batch
    exactly like any other error.
    """
    for table, dump in items:
        assert_gzip(table, dump)
    # staging_name validates each identifier before psql is spawned.
    plans = [
        (table, dump, staging_name(table), pre_sql(table), post_sql(table))
        for table, dump in items
    ]

    env = build_psql_env(settings, region)
    cmd = ["psql", "-v", "ON_ERROR_STOP=1", "--single-transaction", "--quiet"]
    tables = [table for table, _ in items]
    logger.info("Restoring %d table(s) atomically: %s", len(tables), ", ".join(tables))

    # stdout is discarded: the dump preamble's `SELECT pg_catalog.set_config(...)`
    # prints a result table that the CDP log shipper would otherwise index as
    # unparseable stdout with log.level=error. Errors arrive on stderr only.
    proc = subprocess.Popen(  # noqa: S603
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        env=env,
    )
    if proc.stdin is None:
        msg = "failed to open psql stdin"
        raise RuntimeError(msg)
    try:
        # STAGE: every table's staging table + COPY data, in order.
        for table, dump, stage, pre, _post in plans:
            logger.info("Loading table %s from %s", table, dump)
            start = time.perf_counter()
            proc.stdin.write(pre.encode())
            _stream_dump_to_staging(proc.stdin, dump, table, stage)
            logger.info(
                "Streamed table %s in %.2fs", table, time.perf_counter() - start
            )
        # QC: one generated block checking every applicable rule against every
        # staged table, reached only after all tables have staged (referential
        # checks need every side of a pair available).
        if qc_rules is not None:
            # Local import: app.data_sync.qc imports staging_name from this
            # module, so a module-level import here would create an import
            # cycle (restore -> qc -> restore) that fails depending on which
            # module a caller imports first.
            from app.data_sync.qc import build_qc_sql

            proc.stdin.write(build_qc_sql(items, qc_rules).encode())
        # PROMOTE: reached only if QC didn't raise. Not individually timed per table:
        # post_sql's writes are too small to be backpressure-limited by the pipe (unlike
        # STAGE's bulk COPY data), so a per-table timer here would report near-zero
        # durations regardless of actual INSERT/index-maintenance cost, misleadingly
        # implying promotion is cheap. The INSERT/index-maintenance cost (the NRF2-694
        # tripwire) is only visible in aggregate via the "Committed" log below, which
        # also includes QC evaluation time and the final COMMIT.
        for _table, _dump, _stage, _pre, post in plans:
            proc.stdin.write(post.encode())
        proc.stdin.close()
    except BrokenPipeError:  # psql already exited with an error
        pass
    commit_start = time.perf_counter()
    _, stderr = proc.communicate()
    if proc.returncode != 0:
        msg = f"psql atomic restore failed: {stderr.decode(errors='replace')}"
        raise RuntimeError(msg)
    logger.info(
        "Committed %d table(s) in %.2fs: %s",
        len(tables),
        time.perf_counter() - commit_start,
        ", ".join(tables),
    )
