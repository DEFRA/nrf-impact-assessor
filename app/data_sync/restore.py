"""Atomic, whole-manifest restore of gzipped pg_dumps via a single psql txn.

Each dump is data-only (COPY ... FROM stdin). All tables in a run are streamed
into one `psql --single-transaction` process: for each table we drop its
secondary indexes, truncate, replay the COPY data, then recreate the indexes —
faster than COPY into an indexed table. Because the whole batch shares one
transaction, readers see the old data for every table until the final COMMIT,
and any error rolls back all tables (no mixed-version state to reconcile).
"""

import gzip
import logging
import os
import subprocess
from pathlib import Path
from typing import IO

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.config import DatabaseSettings
from app.repositories.repository import _assert_safe_identifier

logger = logging.getLogger(__name__)

# Secondary (non-constraint) indexes on the table — these are safe to DROP/CREATE.
_INDEX_QUERY = text(
    """
    SELECT i.indexname, i.indexdef
    FROM pg_indexes i
    WHERE i.schemaname = 'public' AND i.tablename = :table
      AND NOT EXISTS (
        SELECT 1 FROM pg_constraint c
        WHERE c.conindid = (
            SELECT oid FROM pg_class WHERE relname = i.indexname
        )
      )
    """
)


def get_secondary_indexes(engine: Engine, table: str) -> list[tuple[str, str]]:
    """Return (indexname, indexdef) for non-constraint indexes on the table."""
    with engine.connect() as conn:
        rows = conn.execute(_INDEX_QUERY, {"table": table}).all()
    return [(r[0], r[1]) for r in rows]


def wrap_table(
    table: str, drop_index_sql: list[str], create_index_sql: list[str]
) -> tuple[str, str]:
    """Return (pre, post) SQL fragments that bracket one table's COPY data.

    No BEGIN/COMMIT: the outer transaction is supplied by
    `psql --single-transaction`, which wraps the whole batch.
    """
    pre = "\n".join(drop_index_sql) + f"\nTRUNCATE public.{table};\n"
    post = "\n" + "\n".join(create_index_sql) + "\n"
    return pre, post


def plan_table(engine: Engine, table: str) -> tuple[str, str]:
    """Validate `table` and return its (pre, post) SQL fragments. No DB writes.

    The table name originates from an untrusted S3 manifest and is interpolated
    into raw SQL (TRUNCATE/DROP/CREATE INDEX); validate before it can reach any
    DB call or SQL string.
    """
    _assert_safe_identifier(table, "table")
    indexes = get_secondary_indexes(engine, table)
    drop_sql = [f"DROP INDEX IF EXISTS public.{name};" for name, _ in indexes]
    create_sql = [f"{ddl};" for _, ddl in indexes]
    return wrap_table(table, drop_sql, create_sql)


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
    engine: Engine,
    settings: DatabaseSettings,
    region: str,
    items: list[tuple[str, Path]],
) -> None:
    """Restore every (table, dump) in a single psql transaction. All-or-nothing.

    Validation and index introspection for all tables happen up front, before
    psql is spawned, so an unsafe name aborts the whole batch before any DB or
    subprocess side effect. `--single-transaction` + `ON_ERROR_STOP=1` make the
    batch atomic: the first error rolls back every table.
    """
    # Validate everything for ALL tables before touching psql: gzip content
    # first (cheap, no DB), then identifier + index introspection.
    for table, dump in items:
        assert_gzip(table, dump)
    plans = [(table, dump, *plan_table(engine, table)) for table, dump in items]

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
    try:
        for table, dump, pre, post in plans:
            logger.info("Streaming table %s from %s", table, dump)
            proc.stdin.write(pre.encode())
            _stream_gzip(proc.stdin, dump)
            proc.stdin.write(post.encode())
        proc.stdin.close()
    except BrokenPipeError:  # psql already exited with an error
        pass
    _, stderr = proc.communicate()
    if proc.returncode != 0:
        msg = f"psql atomic restore failed: {stderr.decode(errors='replace')}"
        raise RuntimeError(msg)
