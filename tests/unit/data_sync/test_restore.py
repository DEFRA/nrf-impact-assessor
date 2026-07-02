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


@pytest.fixture
def psql_stdin(monkeypatch):
    """Replace subprocess.Popen with a fake psql whose stdin captures every byte
    restore_all_atomic writes. Yields the capture buffer so tests can assert on
    the generated SQL without spawning a real psql process.
    """
    written = bytearray()

    class _FakeStdin:
        def write(self, data):
            written.extend(data)

        def close(self):
            # No-op: the fake has no real OS pipe to flush or close; we only
            # capture the bytes written. restore_all_atomic calls stdin.close()
            # to signal EOF to psql, so the method must exist but do nothing.
            pass

    class _FakeProc:
        def __init__(self):
            self.stdin = _FakeStdin()
            self.returncode = 0

        def communicate(self):
            return b"", b""

    monkeypatch.setattr(restore_mod.subprocess, "Popen", lambda *a, **k: _FakeProc())  # noqa: ARG005
    return written


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


def test_old_version_cleanup_sql_keeps_latest_two_versions():
    from app.data_sync.restore import old_version_cleanup_sql

    sql = old_version_cleanup_sql("nn_catchments")
    assert "WHERE version < (SELECT MAX(version) FROM public.nn_catchments) - 1" in sql


def test_old_version_cleanup_sql_rejects_unsafe_identifier():
    from app.data_sync.restore import old_version_cleanup_sql

    with pytest.raises(ValueError, match="identifier"):
        old_version_cleanup_sql("nn_catchments; DROP TABLE users; --")


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


def test_restore_all_atomic_writes_qc_block_between_stage_and_promote(
    tmp_path, psql_stdin
):
    from app.data_sync.qc_rules import load_qc_rules

    dump = tmp_path / "nn.sql.gz"
    dump.write_bytes(
        gzip.compress(b"COPY public.nn_catchments (id) FROM stdin;\nabc\n\\.\n")
    )
    settings = restore_mod.DatabaseSettings(iam_authentication=False)

    rules = load_qc_rules()
    restore_mod.restore_all_atomic(
        settings=settings,
        region="eu-west-2",
        items=[("nn_catchments", dump)],
        qc_rules=rules,
    )

    text = psql_stdin.decode()
    stage_idx = text.index("CREATE TEMP TABLE _ds_stage_nn_catchments")
    qc_idx = text.index("DO $qc$")
    promote_idx = text.index("INSERT INTO public.nn_catchments")
    assert stage_idx < qc_idx < promote_idx


def test_restore_all_atomic_omits_qc_block_when_rules_not_supplied(
    tmp_path, psql_stdin
):
    dump = tmp_path / "nn.sql.gz"
    dump.write_bytes(
        gzip.compress(b"COPY public.nn_catchments (id) FROM stdin;\nabc\n\\.\n")
    )
    settings = restore_mod.DatabaseSettings(iam_authentication=False)

    restore_mod.restore_all_atomic(
        settings=settings, region="eu-west-2", items=[("nn_catchments", dump)]
    )
    assert "DO $qc$" not in psql_stdin.decode()


def test_restore_all_atomic_stages_all_tables_before_promoting_any(
    tmp_path, psql_stdin
):
    """With 2+ tables and no qc_rules, both STAGE passes must complete before
    either PROMOTE pass runs (pre1, stream1, pre2, stream2, post1, post2) —
    this is the reordering introduced by the STAGE/QC/PROMOTE restructure,
    and it applies even when qc_rules is None (today's default call shape).
    """
    dump1 = tmp_path / "nn.sql.gz"
    dump1.write_bytes(
        gzip.compress(b"COPY public.nn_catchments (id) FROM stdin;\nabc\n\\.\n")
    )
    dump2 = tmp_path / "lpa.sql.gz"
    dump2.write_bytes(
        gzip.compress(b"COPY public.lpa_boundaries (id) FROM stdin;\ndef\n\\.\n")
    )
    settings = restore_mod.DatabaseSettings(iam_authentication=False)

    restore_mod.restore_all_atomic(
        settings=settings,
        region="eu-west-2",
        items=[("nn_catchments", dump1), ("lpa_boundaries", dump2)],
    )

    text = psql_stdin.decode()
    stage1_idx = text.index("CREATE TEMP TABLE _ds_stage_nn_catchments")
    copy1_idx = text.index("COPY pg_temp._ds_stage_nn_catchments")
    stage2_idx = text.index("CREATE TEMP TABLE _ds_stage_lpa_boundaries")
    copy2_idx = text.index("COPY pg_temp._ds_stage_lpa_boundaries")
    promote1_idx = text.index("INSERT INTO public.nn_catchments")
    promote2_idx = text.index("INSERT INTO public.lpa_boundaries")

    assert stage1_idx < copy1_idx < stage2_idx < copy2_idx < promote1_idx < promote2_idx
