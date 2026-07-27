import gzip
import io
from pathlib import Path
from uuid import uuid4

import pytest

from app.config import DatabaseSettings
from app.data_sync import restore as restore_mod
from app.data_sync.restore import (
    assert_gzip,
    build_psql_env,
    post_sql,
    restore_all_atomic,
    sql_str,
)

_RUN_ID = uuid4()


def _item(table, dumps, key="k/1", etag="etag1", version="v1"):
    """A RestoreItem; `dumps` may be a single Path or a list of part Paths."""
    from app.data_sync.restore import RestoreItem

    return RestoreItem(
        table=table,
        dumps=[dumps] if isinstance(dumps, Path) else list(dumps),
        s3_key=key,
        etag=etag,
        data_version=version,
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
    items = [_item("nn_catchments; DROP TABLE users; --", dump)]

    with pytest.raises(ValueError, match="identifier"):
        restore_all_atomic(
            settings=settings,
            region="eu-west-2",
            items=items,
            run_id=_RUN_ID,
        )


def test_assert_gzip_accepts_gzip_dump(tmp_path):
    dump = tmp_path / "nn.sql.gz"
    dump.write_bytes(gzip.compress(b"COPY public.nn_catchments ...\n"))
    assert_gzip("nn_catchments", [dump])  # must not raise


def test_assert_gzip_rejects_plain_dump(tmp_path):
    """A non-gzip object fails fast with a clear message, not a raw traceback."""
    dump = tmp_path / "nn.sql"
    dump.write_bytes(b"COPY public.nn_catchments ...\n")
    with pytest.raises(ValueError, match="gzip"):
        assert_gzip("nn_catchments", [dump])


def test_assert_gzip_rejects_empty_dump(tmp_path):
    dump = tmp_path / "empty.gz"
    dump.write_bytes(b"")
    with pytest.raises(ValueError, match="gzip"):
        assert_gzip("nn_catchments", [dump])


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


def test_sql_str_escapes_single_quotes():
    assert sql_str("a'b") == "'a''b'"
    assert sql_str("plain") == "'plain'"


def test_post_sql_bumps_version_inserts_and_drops_staging():
    sql = post_sql("nn_catchments", str(_RUN_ID), "k/1", "etag1", "v1")
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


def test_post_sql_emits_history_and_pointer_with_promoted_version():
    sql = post_sql(
        "lpa_boundaries",
        str(_RUN_ID),
        key="20260724/x/y",
        etag='"abc"',
        data_version="20260724_1",
    )
    # data load still present
    assert "INSERT INTO public.lpa_boundaries SELECT * FROM pg_temp." in sql
    # history row: status success, row_version = post-insert MAX(version)
    assert "INSERT INTO public.data_load_history" in sql
    assert "(SELECT MAX(version) FROM public.lpa_boundaries)" in sql
    assert "'success'" in sql
    assert "'20260724_1'" in sql
    assert f"{sql_str(str(_RUN_ID))}" in sql
    # etag preserved as a literal (double-quotes kept)
    assert "'\"abc\"'" in sql
    # pointer upsert
    assert "INSERT INTO public.data_active_version" in sql
    assert "ON CONFLICT (table_name) DO UPDATE" in sql


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
    _stream_dump_to_staging(out, [dump], "nn_catchments", "_ds_stage_nn_catchments")
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
        _stream_dump_to_staging(out, [dump], "nn_catchments", "_ds_stage_nn_catchments")


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
        items=[_item("nn_catchments", dump)],
        run_id=_RUN_ID,
        qc_rules=rules,
        # nn_catchments' referential checks read the unstaged from-side tables.
        active_versions={"lookup_table": 1, "coefficient_layer": 1},
    )

    text = psql_stdin.decode()
    stage_idx = text.index("CREATE TEMP TABLE _ds_stage_nn_catchments")
    qc_idx = text.index("DO $qc$")
    promote_idx = text.index("INSERT INTO public.nn_catchments")
    assert stage_idx < qc_idx < promote_idx


def test_restore_all_atomic_resets_search_path_after_every_dump(tmp_path, psql_stdin):
    """Every streamed dump must be followed by a search_path reset.

    A real `pg_dump` preamble runs `set_config('search_path', '', false)`, which
    the restore streams verbatim into psql; the QC block that follows calls
    PostGIS functions (ST_IsValid and friends) by bare name and would fail with
    "function st_isvalid(public.geometry) does not exist". The reset goes after
    each dump — one before the QC block would leave the next table's staging
    statements running with a blank search_path.
    """
    dump1 = tmp_path / "nn.sql.gz"
    dump1.write_bytes(
        gzip.compress(
            b"SELECT pg_catalog.set_config('search_path', '', false);\n"
            b"COPY public.nn_catchments (id) FROM stdin;\nabc\n\\.\n"
        )
    )
    dump2 = tmp_path / "lpa.sql.gz"
    dump2.write_bytes(
        gzip.compress(
            b"SELECT pg_catalog.set_config('search_path', '', false);\n"
            b"COPY public.lpa_boundaries (id) FROM stdin;\ndef\n\\.\n"
        )
    )
    settings = restore_mod.DatabaseSettings(iam_authentication=False)

    restore_mod.restore_all_atomic(
        settings=settings,
        region="eu-west-2",
        items=[_item("nn_catchments", dump1), _item("lpa_boundaries", dump2)],
        run_id=_RUN_ID,
    )

    text = psql_stdin.decode()
    reset = restore_mod.search_path_sql()
    assert text.count(reset) == 2
    # Each reset lands after its own dump's COPY body, before the next statement.
    copy2_idx = text.index("COPY pg_temp._ds_stage_lpa_boundaries")
    assert text.index(reset) < copy2_idx < text.rindex(reset)


def test_restore_all_atomic_omits_qc_block_when_rules_not_supplied(
    tmp_path, psql_stdin
):
    dump = tmp_path / "nn.sql.gz"
    dump.write_bytes(
        gzip.compress(b"COPY public.nn_catchments (id) FROM stdin;\nabc\n\\.\n")
    )
    settings = restore_mod.DatabaseSettings(iam_authentication=False)

    restore_mod.restore_all_atomic(
        settings=settings,
        region="eu-west-2",
        items=[_item("nn_catchments", dump)],
        run_id=_RUN_ID,
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
        items=[_item("nn_catchments", dump1), _item("lpa_boundaries", dump2)],
        run_id=_RUN_ID,
    )

    text = psql_stdin.decode()
    stage1_idx = text.index("CREATE TEMP TABLE _ds_stage_nn_catchments")
    copy1_idx = text.index("COPY pg_temp._ds_stage_nn_catchments")
    stage2_idx = text.index("CREATE TEMP TABLE _ds_stage_lpa_boundaries")
    copy2_idx = text.index("COPY pg_temp._ds_stage_lpa_boundaries")
    promote1_idx = text.index("INSERT INTO public.nn_catchments")
    promote2_idx = text.index("INSERT INTO public.lpa_boundaries")

    assert stage1_idx < copy1_idx < stage2_idx < copy2_idx < promote1_idx < promote2_idx


def _split_bytes(data: bytes, tmp_path, chunk: int, name="d.sql.gz"):
    """Write `data` to `tmp_path` as fixed-size parts, mimicking `split -b`."""
    paths = []
    suffixes = ("aa", "ab", "ac", "ad", "ae", "af")
    for i in range(0, len(data), chunk):
        p = tmp_path / f"{name}.part-{suffixes[i // chunk]}"
        p.write_bytes(data[i : i + chunk])
        paths.append(p)
    return paths


def test_chained_reader_reads_across_part_boundaries(tmp_path):
    from app.data_sync.restore import _ChainedReader

    data = bytes(range(256)) * 4
    paths = _split_bytes(data, tmp_path, chunk=300)
    assert len(paths) > 3
    r = _ChainedReader(paths)
    # A read spanning three parts returns the full count, not a short read.
    assert r.read(750) == data[:750]
    assert r.read(-1) == data[750:]
    assert r.read(10) == b""
    r.close()


def test_chained_reader_read_at_exact_boundary(tmp_path):
    from app.data_sync.restore import _ChainedReader

    data = b"A" * 50 + b"B" * 50
    r = _ChainedReader(_split_bytes(data, tmp_path, chunk=50))
    assert r.read(50) == b"A" * 50
    assert r.read(50) == b"B" * 50
    assert r.read(1) == b""
    r.close()


def test_chained_reader_single_part_matches_plain_file(tmp_path):
    from app.data_sync.restore import _ChainedReader

    p = tmp_path / "only.gz"
    p.write_bytes(b"hello world")
    r = _ChainedReader([p])
    assert r.read(-1) == b"hello world"
    r.close()


def test_chained_reader_skips_empty_parts(tmp_path):
    from app.data_sync.restore import _ChainedReader

    a = tmp_path / "a"
    a.write_bytes(b"xy")
    empty = tmp_path / "b"
    empty.write_bytes(b"")
    c = tmp_path / "c"
    c.write_bytes(b"z")
    r = _ChainedReader([a, empty, c])
    assert r.read(3) == b"xyz"
    r.close()


def test_chained_reader_read_zero_does_not_skip_a_part(tmp_path):
    """read(0) must return b"" without consuming or advancing past a part."""
    from app.data_sync.restore import _ChainedReader

    r = _ChainedReader(_split_bytes(b"AB" + b"CD", tmp_path, chunk=2))
    assert r.read(0) == b""
    assert r.read(4) == b"ABCD"
    r.close()


def test_chained_reader_rejects_an_empty_path_list():
    from app.data_sync.restore import _ChainedReader

    with pytest.raises(ValueError, match="at least one part"):
        _ChainedReader([])


@pytest.mark.parametrize("parts", [2, 3, 5, 17])
def test_gzip_round_trip_through_chained_reader(tmp_path, parts):
    """The real contract: a gzip member sliced at an arbitrary offset
    decompresses back to the original through the chained reader.
    """
    from app.data_sync.restore import _ChainedReader

    body = b"".join(f"row-{i}\tvalue-{i}\n".encode() for i in range(500))
    blob = gzip.compress(body)
    chunk = len(blob) // parts + 1
    paths = []
    for i in range(0, len(blob), chunk):
        p = tmp_path / f"d.sql.gz.part-{i // chunk:03d}"
        p.write_bytes(blob[i : i + chunk])
        paths.append(p)
    assert len(paths) == parts
    with gzip.GzipFile(fileobj=_ChainedReader(paths)) as gz:
        assert gz.read() == body


def test_assert_gzip_checks_only_the_first_part(tmp_path):
    """Parts 2..N are raw byte slices with no magic bytes — checking them would
    fail every valid split dump.
    """
    blob = gzip.compress(b"COPY public.nn_catchments (id) FROM stdin;\nabc\n\\.\n")
    parts = _split_bytes(blob, tmp_path, chunk=max(1, len(blob) // 2 + 1))
    assert len(parts) > 1
    assert_gzip("nn_catchments", parts)  # must not raise


def test_assert_gzip_names_the_first_part_when_it_is_not_gzip(tmp_path):
    bad = tmp_path / "d.sql.gz.part-aa"
    bad.write_bytes(b"COPY public.nn_catchments")
    with pytest.raises(ValueError, match="part-aa"):
        assert_gzip("nn_catchments", [bad, tmp_path / "d.sql.gz.part-ab"])


def test_stream_dump_to_staging_handles_a_split_dump(tmp_path):
    """A COPY header straddling a part boundary still gets rewritten, because
    the rewrite happens after decompression of the joined stream.
    """
    from app.data_sync.restore import _stream_dump_to_staging

    body = (
        b"--\n-- preamble\n--\n"
        b"SELECT pg_catalog.set_config('search_path', '', false);\n"
        b"COPY public.nn_catchments (id, version) FROM stdin;\n"
        + b"".join(f"row{i}\t1\n".encode() for i in range(200))
        + b"\\.\n"
    )
    blob = gzip.compress(body)
    parts = _split_bytes(blob, tmp_path, chunk=len(blob) // 4 + 1)
    assert len(parts) >= 4

    out = io.BytesIO()
    _stream_dump_to_staging(out, parts, "nn_catchments", "_ds_stage_nn_catchments")
    written = out.getvalue()

    assert (
        b"COPY pg_temp._ds_stage_nn_catchments (id, version) FROM stdin;\n" in written
    )
    assert b"COPY public.nn_catchments" not in written
    assert b"row0\t1\n" in written
    assert b"row199\t1\n" in written
    assert b"\\.\n" in written


def test_split_and_single_dumps_stream_identically(tmp_path):
    """The proof that splitting is transparent: identical bytes reach psql."""
    from app.data_sync.restore import _stream_dump_to_staging

    body = (
        b"COPY public.nn_catchments (id) FROM stdin;\n"
        + b"".join(f"row{i}\n".encode() for i in range(300))
        + b"\\.\n"
    )
    blob = gzip.compress(body)
    whole = tmp_path / "whole.sql.gz"
    whole.write_bytes(blob)
    parts = _split_bytes(blob, tmp_path, chunk=len(blob) // 3 + 1)

    single_out, split_out = io.BytesIO(), io.BytesIO()
    _stream_dump_to_staging(
        single_out, [whole], "nn_catchments", "_ds_stage_nn_catchments"
    )
    _stream_dump_to_staging(
        split_out, parts, "nn_catchments", "_ds_stage_nn_catchments"
    )
    assert single_out.getvalue() == split_out.getvalue()
