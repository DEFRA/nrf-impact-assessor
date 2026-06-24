import gzip
import io

import pytest

from app.config import DatabaseSettings
from app.data_sync import restore as restore_mod
from app.data_sync.restore import (
    assert_gzip,
    build_psql_env,
    restore_all_atomic,
)


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


def test_staging_name_is_derived_and_validated():
    from app.data_sync.restore import staging_name

    assert staging_name("nn_catchments") == "_ds_stage_nn_catchments"
    with pytest.raises(ValueError, match="identifier"):
        staging_name("nn; DROP TABLE users; --")


def test_pre_sql_creates_temp_staging_like_live_table():
    from app.data_sync.restore import pre_sql

    sql = pre_sql("nn_catchments")
    assert (
        "CREATE TEMP TABLE _ds_stage_nn_catchments (LIKE public.nn_catchments);" in sql
    )
    assert "BEGIN;" not in sql


def test_post_sql_bumps_version_inserts_and_drops_staging():
    from app.data_sync.restore import post_sql

    sql = post_sql("nn_catchments")
    assert "UPDATE pg_temp._ds_stage_nn_catchments" in sql
    assert "id = gen_random_uuid()" in sql
    assert "COALESCE(MAX(version),0)+1 FROM public.nn_catchments" in sql
    assert (
        "INSERT INTO public.nn_catchments SELECT * FROM pg_temp._ds_stage_nn_catchments;"
        in sql
    )
    assert "DROP TABLE pg_temp._ds_stage_nn_catchments;" in sql
    assert "BEGIN;" not in sql
    assert "COMMIT;" not in sql


def test_old_version_cleanup_sql_keeps_only_latest():
    from app.data_sync.restore import old_version_cleanup_sql

    sql = old_version_cleanup_sql("nn_catchments")
    assert sql == (
        "DELETE FROM public.nn_catchments "
        "WHERE version < (SELECT MAX(version) FROM public.nn_catchments);"
    )
    with pytest.raises(ValueError, match="identifier"):
        old_version_cleanup_sql("nn; DROP TABLE users; --")


def test_rewrite_copy_line_redirects_only_the_header():
    from app.data_sync.restore import _rewrite_copy_line

    header = b"COPY public.nn_catchments (id, version, name) FROM stdin;\n"
    rewritten = _rewrite_copy_line(header, "nn_catchments", "_ds_stage_nn_catchments")
    assert rewritten == (
        b"COPY pg_temp._ds_stage_nn_catchments (id, version, name) FROM stdin;\n"
    )
    # A data row that merely contains the table name is left untouched.
    data = b"abc\t1\tpublic.nn_catchments stuff\n"
    assert _rewrite_copy_line(data, "nn_catchments", "_ds_stage_nn_catchments") == data


def test_stream_dump_to_staging_rewrites_header_and_preserves_body(tmp_path):
    from app.data_sync.restore import _stream_dump_to_staging

    body = (
        b"--\n-- preamble\n--\n"
        b"SELECT pg_catalog.set_config('search_path', '', false);\n"
        b"COPY public.nn_catchments (id, version) FROM stdin;\n"
        b"abc\t1\n\\.\n"
    )
    dump = tmp_path / "nn.sql.gz"
    dump.write_bytes(gzip.compress(body))

    out = io.BytesIO()
    _stream_dump_to_staging(out, dump, "nn_catchments", "_ds_stage_nn_catchments")
    written = out.getvalue()

    assert (
        b"COPY pg_temp._ds_stage_nn_catchments (id, version) FROM stdin;\n" in written
    )
    assert b"COPY public.nn_catchments" not in written
    assert b"SELECT pg_catalog.set_config" in written  # preamble preserved
    assert b"abc\t1\n" in written  # data preserved
    assert b"\\.\n" in written  # terminator preserved


def test_stream_dump_to_staging_raises_without_copy_header(tmp_path):
    from app.data_sync.restore import _stream_dump_to_staging

    dump = tmp_path / "bad.sql.gz"
    dump.write_bytes(gzip.compress(b"-- preamble only, no COPY line\n"))
    out = io.BytesIO()
    with pytest.raises(ValueError, match="COPY header"):
        _stream_dump_to_staging(out, dump, "nn_catchments", "_ds_stage_nn_catchments")
