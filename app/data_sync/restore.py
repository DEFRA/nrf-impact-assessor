"""Versioned, whole-manifest restore of gzipped data-only pg_dumps.

Each dump is `COPY ... FROM stdin`. All tables in a run stream into one psql
process wrapped in an explicit `BEGIN` / `COMMIT`. Per table we: create a TEMP
staging table shaped like the live table, redirect the dump's COPY into that
staging table, then stamp a fresh id + `version = MAX(version)+1` and
`INSERT ... SELECT *` into the live table. Because the whole batch shares one
transaction, readers keep seeing the prior version of every table until the
final COMMIT, then flip to the new version together; any error rolls back all
tables.

The transaction is opened and closed by statements we write, NOT by
`psql --single-transaction`. That flag commits at end of input, and stdin
reaching EOF because the parent *died* is indistinguishable to psql from an
orderly finish — so a parent killed partway through the PROMOTE loop would have
psql commit the promotions it had already received, leaving the batch
half-applied at mixed versions. With an explicit `COMMIT` as the last statement,
a premature EOF ends the session with the transaction still open and the server
rolls it back. See tests/integration/data_sync/test_psql_eof_commit_window.py.

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
from dataclasses import dataclass
from pathlib import Path
from typing import IO
from uuid import UUID

from app.config import DatabaseSettings
from app.data_sync.qc_rules import QcRules
from app.repositories.repository import _assert_safe_identifier

logger = logging.getLogger(__name__)


_STAGE_PREFIX = "_ds_stage_"


@dataclass(frozen=True)
class RestoreItem:
    """One table's contribution to a restore batch.

    `dumps` holds the table's dump parts in concatenation order — a single
    entry for an unsplit dump. `s3_key` and `etag` are the values recorded in
    `data_load_history` (for a split dump, a base key plus a composite ETag;
    see app/data_sync/service.py).
    """

    table: str
    dumps: list[Path]
    s3_key: str
    etag: str
    data_version: str


def staging_name(table: str) -> str:
    """Return the temp staging table name for `table` (validated)."""
    _assert_safe_identifier(table, "table")
    return f"{_STAGE_PREFIX}{table}"


def restore_lock_key(lock_key: int) -> int:
    """The advisory-lock key psql holds, derived from the run's `lock_key`.

    Must differ from the key the parent holds: the parent takes its lock for the
    whole run on its own connection, so psql asking for the same key would block
    forever waiting on a session that will not release until psql finishes.
    """
    return lock_key + 1


def begin_sql(lock_key: int) -> str:
    """Open the restore transaction and take psql's own liveness lock.

    `pg_advisory_xact_lock` (transaction-scoped, not session-scoped) is held for
    exactly as long as the restore transaction: the server drops it on COMMIT or
    on the rollback that follows a lost connection. That makes it a liveness
    signal for the *child*, which is what run reclamation needs — the parent's
    session lock disappears the moment the parent is killed, but an orphaned psql
    can outlive it (see app/data_sync/router.py::_reclaim_orphaned_run).
    """
    return f"BEGIN;\nSELECT pg_advisory_xact_lock({lock_key});\n"


def commit_sql() -> str:
    """Close the restore transaction.

    The last thing written to psql's stdin. If the parent dies before this is
    written, psql sees EOF with the transaction still open and the server rolls
    the whole batch back.
    """
    return "COMMIT;\n"


def pre_sql(table: str) -> str:
    """SQL emitted before a table's COPY data: create the temp staging table.

    Columns only (no indexes) for a fast staging load. CREATE TEMP always
    targets pg_temp regardless of search_path. No BEGIN/COMMIT — the outer
    transaction is supplied by `begin_sql()` / `commit_sql()`.
    """
    stage = staging_name(table)
    return f"CREATE TEMP TABLE {stage} (LIKE public.{table});\n"


def search_path_sql() -> str:
    """SQL that restores a usable search_path after a dump has been streamed.

    A data-only `pg_dump` opens with `SELECT pg_catalog.set_config('search_path',
    '', false)` and the restore streams that preamble straight into psql, so from
    the first dump onwards the session resolves *nothing* unqualified. Our own
    statements are schema-qualified, but the QC block calls PostGIS functions
    (`ST_IsValid`, `ST_SRID`, `GeometryType`, ...) by bare name and they live in
    `public` alongside the `geometry` type — without this reset they fail with
    "function st_isvalid(public.geometry) does not exist".
    """
    return "SET search_path TO public, pg_temp;\n"


def sql_str(value: str) -> str:
    """Return `value` as a single-quoted SQL string literal, quotes escaped."""
    return "'" + value.replace("'", "''") + "'"


def post_sql(table: str, run_id: str, key: str, etag: str, data_version: str) -> str:
    """SQL emitted after a table's COPY data: stamp a fresh id + new version,
    load the live table from staging, drop staging, then — in the same
    transaction — record the DataLoadHistory row and promote the active-version
    pointer.

    `id` is regenerated (no FK references these ids) to avoid PK collisions with
    the rows already present; `version` is MAX(version)+1 computed once against
    the pre-insert snapshot. `LIKE` preserves column order, so `SELECT *` aligns.

    History + pointer are written here (not on a separate ORM commit) so that the
    data, its audit row, and the active-version cutover commit or roll back as
    one unit: `MAX(version)` after the INSERT is exactly the version just
    promoted (retention keeps MAX and MAX-1), so both statements reference it
    directly. Callers pass already-known values (`run_id`, `key`, `etag`,
    `data_version`); they are emitted as escaped SQL literals via `sql_str`.
    """
    stage = staging_name(table)
    # noqa justified: identifiers validated by staging_name; values via sql_str.
    max_v = f"(SELECT MAX(version) FROM public.{table})"  # noqa: S608
    sql = (
        f"UPDATE pg_temp.{stage} SET id = gen_random_uuid(), "  # noqa: S608
        f"version = (SELECT COALESCE(MAX(version),0)+1 FROM public.{table});\n"
        f"INSERT INTO public.{table} SELECT * FROM pg_temp.{stage};\n"
        f"DROP TABLE pg_temp.{stage};\n"
        f"INSERT INTO public.data_load_history "
        f"(id, run_id, table_name, s3_key, etag, data_version, status, "
        f"row_version, loaded_at) "
        f"VALUES (gen_random_uuid(), {sql_str(run_id)}, {sql_str(table)}, "
        f"{sql_str(key)}, {sql_str(etag)}, {sql_str(data_version)}, 'success', "
        f"{max_v}, now());\n"
        f"INSERT INTO public.data_active_version "
        f"(table_name, active_version, updated_at) "
        f"VALUES ({sql_str(table)}, {max_v}, now()) "
        f"ON CONFLICT (table_name) DO UPDATE SET "
        f"active_version = EXCLUDED.active_version, updated_at = now();\n"
    )
    return sql


def old_version_cleanup_sql(table: str) -> str:
    """SQL that deletes every version older than the retained pair.

    Retention keeps MAX(version) and MAX(version)-1 (not latest-only), so a
    rollback (app/data_sync/active_version.py) always has a previous version's
    rows to point back at.
    """
    _assert_safe_identifier(table, "table")
    # noqa justified: identifier validated by _assert_safe_identifier
    sql = (
        f"DELETE FROM public.{table} "  # noqa: S608
        f"WHERE version < (SELECT MAX(version) FROM public.{table}) - 1;"
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


class _ChainedReader:
    """Read-only binary stream presenting `paths` read end-to-end as one file.

    `split -b` slices a single gzip member across files, so the parts are only
    decompressable as one concatenated stream — no part except the first even
    carries the gzip magic bytes. Chaining at read time (rather than `cat`-ing
    the parts into a second file) keeps the restore's disk footprint at one copy
    of the dump: a 3 GB dump needs 3 GB of container disk, not 6 GB.

    Only `read`/`close` are implemented — that is the whole surface
    `gzip.GzipFile(fileobj=...)` uses for reading.
    """

    def __init__(self, paths: list[Path]) -> None:
        if not paths:
            msg = "_ChainedReader requires at least one part path"
            raise ValueError(msg)
        self._paths = list(paths)
        self._index = 0
        self._fh: IO[bytes] | None = paths[0].open("rb")

    def _advance(self) -> bool:
        """Close the current part and open the next. False when none remain."""
        if self._fh is not None:
            self._fh.close()
            self._fh = None
        self._index += 1
        if self._index >= len(self._paths):
            return False
        self._fh = self._paths[self._index].open("rb")
        return True

    def read(self, size: int = -1) -> bytes:
        """Return `size` bytes, crossing part boundaries as needed.

        Returns fewer bytes only at true end-of-stream. A short read at a part
        boundary would look like EOF to GzipFile and truncate the dump, so the
        loop keeps pulling from subsequent parts until the count is satisfied.
        """
        if size == 0:
            # Guard: without this the loop below reads b"" from the current part
            # and treats it as EOF, silently skipping to the next one.
            return b""
        chunks: list[bytes] = []
        remaining = size
        while self._fh is not None:
            data = self._fh.read() if remaining < 0 else self._fh.read(remaining)
            if data:
                chunks.append(data)
                if remaining >= 0:
                    remaining -= len(data)
                    if remaining == 0:
                        break
            elif not self._advance():
                break
        return b"".join(chunks)

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None
        self._index = len(self._paths)


def assert_gzip(table: str, dumps: list[Path]) -> None:
    """Fail fast with a clear error if a dump is not gzip-compressed.

    Only the FIRST part is checked. `split -b` slices one gzip member, so parts
    2..N legitimately begin mid-stream with no magic bytes; checking them would
    reject every valid split dump. Checking the first part up front still turns
    an opaque mid-transaction decompression error into an actionable message
    naming the offending table.
    """
    first = dumps[0]
    with first.open("rb") as f:
        magic = f.read(2)
    if magic != _GZIP_MAGIC:
        where = (
            f" (first part {first.name!r} of {len(dumps)})" if len(dumps) > 1 else ""
        )
        msg = (
            f"dump for table {table!r} is not gzip-compressed{where} "
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
    stdin: IO[bytes], dumps: list[Path], table: str, stage: str
) -> None:
    """Stream a gzipped data-only dump into psql, redirecting its single COPY
    header to `pg_temp.<stage>`. The (small) preamble is read line-by-line until
    the header; the (large) data body is then streamed in 1 MiB chunks.

    `dumps` is the table's parts in order; they are joined into one byte stream
    before decompression (see `_ChainedReader`), so a split dump is
    indistinguishable from an unsplit one from here down — including a COPY
    header that straddles a part boundary.
    """
    prefix = f"COPY public.{table} ".encode()
    with gzip.GzipFile(fileobj=_ChainedReader(dumps)) as gz:
        found = False
        for line in gz:
            if line.startswith(prefix):
                stdin.write(_rewrite_copy_line(line, table, stage))
                found = True
                break
            stdin.write(line)
        if not found:
            msg = f"no COPY header for table {table!r} found in dump {dumps[0]}"
            raise ValueError(msg)
        for chunk in iter(lambda: gz.read(1024 * 1024), b""):
            stdin.write(chunk)


def restore_all_atomic(
    settings: DatabaseSettings,
    region: str,
    items: list[RestoreItem],
    run_id: UUID,
    lock_key: int,
    qc_rules: QcRules | None = None,
    active_versions: dict[str, int] | None = None,
) -> None:
    """Load every table in a single psql transaction. All-or-nothing.

    `items` are `RestoreItem`s (see the dataclass above). Within
    the same transaction each table's data load also writes its DataLoadHistory
    row and promotes the active-version pointer (see `post_sql`), so data,
    audit, and cutover commit or roll back together.

    Validation for all tables happens up front (via staging_name), before psql
    is spawned, so an unsafe name aborts the whole batch before any subprocess
    side effect. An explicit `BEGIN` / `COMMIT` pair (see `begin_sql`) plus
    `ON_ERROR_STOP=1` make the batch atomic: the first error rolls back every
    table, and so does a parent death before `COMMIT` is written.

    When `qc_rules` is supplied, a generated QC `DO` block (see
    `app.data_sync.qc.build_qc_sql`) runs after every table has staged and
    before any table promotes, so a QC failure rolls back the whole batch
    exactly like any other error.
    """
    for item in items:
        assert_gzip(item.table, item.dumps)
    # staging_name validates each identifier before psql is spawned.
    plans = [
        (
            item.table,
            item.dumps,
            staging_name(item.table),
            pre_sql(item.table),
            post_sql(
                item.table,
                str(run_id),
                item.s3_key,
                item.etag,
                item.data_version,
            ),
        )
        for item in items
    ]

    env = build_psql_env(settings, region)
    # No --single-transaction: that flag commits at end of input, so a parent
    # killed mid-PROMOTE would have psql commit a half-applied batch. The
    # transaction is opened and closed explicitly instead (see begin_sql).
    cmd = ["psql", "-v", "ON_ERROR_STOP=1", "--quiet"]
    tables = [item.table for item in items]
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
        # BEGIN + psql's own liveness lock, before any work.
        proc.stdin.write(begin_sql(restore_lock_key(lock_key)).encode())
        # STAGE: every table's staging table + COPY data, in order.
        for table, dumps, stage, pre, _post in plans:
            logger.info("Loading table %s from %d part(s)", table, len(dumps))
            start = time.perf_counter()
            proc.stdin.write(pre.encode())
            _stream_dump_to_staging(proc.stdin, dumps, table, stage)
            # Undo the dump preamble's search_path blanking (see search_path_sql).
            # Emitted after every dump, not once before QC, so any statement that
            # follows a dump — this loop's next CREATE TEMP included — runs with a
            # sane search_path.
            proc.stdin.write(search_path_sql().encode())
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

            proc.stdin.write(
                build_qc_sql(items, qc_rules, active_versions or {}).encode()
            )
        # PROMOTE: reached only if QC didn't raise. Not individually timed per table:
        # post_sql's writes are too small to be backpressure-limited by the pipe (unlike
        # STAGE's bulk COPY data), so a per-table timer here would report near-zero
        # durations regardless of actual INSERT/index-maintenance cost, misleadingly
        # implying promotion is cheap. The INSERT/index-maintenance cost (the NRF2-694
        # tripwire) is only visible in aggregate via the "Committed" log below, which
        # also includes QC evaluation time and the final COMMIT.
        for _table, _dumps, _stage, _pre, post in plans:
            proc.stdin.write(post.encode())
        # COMMIT last: everything above is durable only once this is written, so
        # a parent that dies at any earlier point leaves the batch rolled back.
        proc.stdin.write(commit_sql().encode())
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
