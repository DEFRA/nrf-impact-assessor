import gzip

import pytest

from app.config import DatabaseSettings
from app.data_sync import restore as restore_mod
from app.data_sync.restore import (
    assert_gzip,
    build_psql_env,
    plan_table,
    restore_all_atomic,
)


def test_plan_table_rejects_unsafe_table_name():
    """A malicious/invalid table name must be rejected before any DB/SQL runs."""
    with pytest.raises(ValueError, match="identifier"):
        restore_mod.plan_table("nn_catchments; DROP TABLE users; --")


def test_restore_all_atomic_rejects_unsafe_table_before_psql(tmp_path, monkeypatch):
    """An unsafe name anywhere in the batch aborts before psql is spawned."""
    dump = tmp_path / "x.sql.gz"
    dump.write_bytes(gzip.compress(b"COPY ...\n"))  # valid gzip; isolate the name check
    settings = DatabaseSettings(iam_authentication=False)

    def _boom(*_a, **_k):
        pytest.fail("psql must not be spawned when validation fails")

    monkeypatch.setattr(restore_mod.subprocess, "Popen", _boom)

    with pytest.raises(ValueError, match="identifier"):
        restore_all_atomic(
            settings=settings,
            region="eu-west-2",
            items=[("nn_catchments; DROP TABLE users; --", dump)],
        )


def test_assert_gzip_accepts_gzip_dump(tmp_path):
    dump = tmp_path / "nn.sql.gz"
    dump.write_bytes(gzip.compress(b"COPY public.nn_catchments ...\n"))
    assert_gzip("nn_catchments", dump)  # must not raise


def test_assert_gzip_rejects_plain_dump(tmp_path):
    """A non-gzip object fails fast with a clear message, not a raw traceback."""
    dump = tmp_path / "nn.sql"
    dump.write_bytes(b"COPY public.nn_catchments ...\n")
    with pytest.raises(ValueError, match="gzip"):
        assert_gzip("nn_catchments", dump)


def test_assert_gzip_rejects_empty_dump(tmp_path):
    dump = tmp_path / "empty.gz"
    dump.write_bytes(b"")
    with pytest.raises(ValueError, match="gzip"):
        assert_gzip("nn_catchments", dump)


def test_plan_table_truncates_without_index_management():
    """Per-table SQL truncates the table but does not touch indexes.

    Index management is Liquibase's responsibility; the app user does not own
    the tables and cannot DROP/CREATE indexes. No own transaction either: the
    single outer transaction is provided by psql --single-transaction.
    """
    pre = plan_table("nn_catchments")
    assert "BEGIN;" not in pre
    assert "COMMIT;" not in pre
    assert "TRUNCATE public.nn_catchments;" in pre
    assert "DROP INDEX" not in pre
    assert "CREATE INDEX" not in pre


def test_build_psql_env_local_password():
    from uuid import uuid4

    # Generated at runtime so there is no hard-coded credential literal.
    secret = uuid4().hex
    settings = DatabaseSettings(
        host="localhost",
        port=5434,
        database="nrf_impact",
        user="postgres",
        iam_authentication=False,
        local_password=secret,
    )
    env = build_psql_env(settings, region="eu-west-2")
    assert env["PGHOST"] == "localhost"
    assert env["PGPORT"] == "5434"
    assert env["PGDATABASE"] == "nrf_impact"
    assert env["PGUSER"] == "postgres"
    assert env["PGPASSWORD"] == secret
