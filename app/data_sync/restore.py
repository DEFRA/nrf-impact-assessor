"""Atomic, whole-manifest restore of gzipped pg_dumps via a single psql txn.

Each dump is data-only (COPY ... FROM stdin). All tables in a run are streamed
into one `psql --single-transaction` process: for each table we truncate, then
replay the COPY data. Because the whole batch shares one transaction, readers
see the old data for every table until the final COMMIT, and any error rolls
back all tables (no mixed-version state to reconcile).

Indexes are defined once by Liquibase migrations and persist permanently;
Liquibase is not in the data-sync runtime path. Each COPY loads into the live,
indexed tables, so index maintenance happens inline during the load.

Perf note: an earlier version dropped each table's secondary indexes before
COPY and recreated them afterwards (a faster bulk load into an unindexed table),
but DROP/CREATE INDEX requires table ownership, which the data-sync role
(nrf_impact_assessor) does not have. We deliberately accept inline index
maintenance instead. This has not been benchmarked against the largest layers
(e.g. subcatchments, nn_catchments); if load time becomes a problem, the
permission-preserving fallback is a SECURITY DEFINER index-reset function owned
by the DDL role, called by the app role (see NRF2-694 review).
"""

import gzip
import logging
import os
import subprocess
import time
from pathlib import Path
from typing import IO

from app.config import DatabaseSettings
from app.repositories.repository import _assert_safe_identifier

logger = logging.getLogger(__name__)


def plan_table(table: str) -> str:
    """Validate `table` and return the SQL that precedes its COPY data.

    The table name originates from an untrusted S3 manifest and is interpolated
    into raw SQL (TRUNCATE); validate before it can reach any DB call or SQL
    string. No BEGIN/COMMIT: the outer transaction is supplied by
    `psql --single-transaction`, which wraps the whole batch.
    """
    _assert_safe_identifier(table, "table")
    return f"TRUNCATE public.{table};\n"


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


def _stream_gzip(stdin: IO[bytes], dump_path: Path) -> None:
    with gzip.open(dump_path, "rb") as gz:
        for chunk in iter(lambda: gz.read(1024 * 1024), b""):
            stdin.write(chunk)


def restore_all_atomic(
    settings: DatabaseSettings,
    region: str,
    items: list[tuple[str, Path]],
) -> None:
    """Restore every (table, dump) in a single psql transaction. All-or-nothing.

    Validation for all tables happens up front, before psql is spawned, so an
    unsafe name aborts the whole batch before any subprocess side effect.
    `--single-transaction` + `ON_ERROR_STOP=1` make the batch atomic: the
    first error rolls back every table.
    """
    for table, dump in items:
        assert_gzip(table, dump)
    plans = [(table, dump, plan_table(table)) for table, dump in items]

    env = build_psql_env(settings, region)
    cmd = ["psql", "-v", "ON_ERROR_STOP=1", "--single-transaction", "--quiet"]
    tables = [table for table, _ in items]
    logger.info("Restoring %d table(s) atomically: %s", len(tables), ", ".join(tables))

    proc = subprocess.Popen(  # noqa: S603
        cmd, stdin=subprocess.PIPE, stderr=subprocess.PIPE, env=env
    )
    if proc.stdin is None:
        msg = "failed to open psql stdin"
        raise RuntimeError(msg)
    # Per-table durations measure time to feed the dump into psql's stdin. Under
    # --single-transaction the pipe back-pressures, so this reflects feed + apply
    # (incl. inline index maintenance), but the final COMMIT cost is deferred to
    # communicate() below and logged separately. These numbers are the tripwire
    # for the index-maintenance trade-off documented in the module docstring.
    try:
        for table, dump, pre in plans:
            logger.info("Streaming table %s from %s", table, dump)
            start = time.perf_counter()
            proc.stdin.write(pre.encode())
            _stream_gzip(proc.stdin, dump)
            logger.info(
                "Streamed table %s in %.2fs", table, time.perf_counter() - start
            )
        proc.stdin.close()
    except BrokenPipeError:  # psql already exited with an error
        pass
    commit_start = time.perf_counter()
    _, stderr = proc.communicate()
    if proc.returncode != 0:
        msg = f"psql atomic restore failed: {stderr.decode(errors='replace')}"
        raise RuntimeError(msg)
    logger.info(
        "Committed %d table(s) in %.2fs",
        len(tables),
        time.perf_counter() - commit_start,
    )
