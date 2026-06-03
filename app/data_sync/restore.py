"""Per-table transactional restore of a gzipped pg_dump via psql.

The dump is data-only (COPY ... FROM stdin). We wrap it in a single
transaction that drops the table's secondary indexes, truncates, replays
the COPY data, then recreates the indexes — faster than COPY into an
indexed table, and atomic so readers see old data until COMMIT.
"""

import gzip
import logging
import subprocess
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.config import DatabaseSettings

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


def wrap_sql(
    table: str, drop_index_sql: list[str], create_index_sql: list[str]
) -> tuple[str, str]:
    """Return (pre, post) SQL fragments that bracket the dump's COPY data."""
    pre = "BEGIN;\n" + "\n".join(drop_index_sql) + f"\nTRUNCATE public.{table};\n"
    post = "\n" + "\n".join(create_index_sql) + "\nCOMMIT;\n"
    return pre, post


def build_psql_env(settings: DatabaseSettings, region: str) -> dict[str, str]:
    """Build PG* environment for a psql subprocess from DatabaseSettings."""
    import os

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


def restore_table(
    engine: Engine,
    settings: DatabaseSettings,
    region: str,
    table: str,
    dump_path: Path,
) -> None:
    """Restore one table from a gzipped data-only dump, transactionally."""
    indexes = get_secondary_indexes(engine, table)
    drop_sql = [f"DROP INDEX IF EXISTS public.{name};" for name, _ in indexes]
    create_sql = [f"{ddl};" for _, ddl in indexes]
    pre, post = wrap_sql(table, drop_sql, create_sql)

    env = build_psql_env(settings, region)
    cmd = ["psql", "-v", "ON_ERROR_STOP=1", "--quiet"]
    logger.info("Restoring table %s from %s", table, dump_path)

    proc = subprocess.Popen(  # noqa: S603
        cmd, stdin=subprocess.PIPE, stderr=subprocess.PIPE, env=env
    )
    if proc.stdin is None:
        msg = "failed to open psql stdin"
        raise RuntimeError(msg)
    try:
        proc.stdin.write(pre.encode())
        with gzip.open(dump_path, "rb") as gz:
            for chunk in iter(lambda: gz.read(1024 * 1024), b""):
                proc.stdin.write(chunk)
        proc.stdin.write(post.encode())
        proc.stdin.close()
    except BrokenPipeError:  # psql already exited with an error
        pass
    _, stderr = proc.communicate()
    if proc.returncode != 0:
        msg = f"psql restore of {table} failed: {stderr.decode(errors='replace')}"
        raise RuntimeError(msg)
