"""End-to-end data-sync reload against LocalStack S3 + test Postgres."""

import contextlib
import gzip
import os
from uuid import uuid4

import boto3
import pytest
from sqlalchemy import text

from app.config import AWSConfig
from app.data_sync.manifest import Manifest
from app.data_sync.service import run_data_sync
from tests.integration.data_sync.dumps import PG_DUMP_PREAMBLE

pytestmark = pytest.mark.integration

BUCKET = "nrf-ref-data-test"
# Minimal data-only dump: two rows into nn_catchments. The restore truncates
# the table and replays this COPY data; indexes are left untouched (Liquibase
# owns them).
DUMP_SQL = (
    PG_DUMP_PREAMBLE
    + "COPY public.nn_catchments (id, version, geometry, name, attributes, created_at) "
    "FROM stdin;\n"
    f"{uuid4()}\t1\t"
    "0103000020346C00000100000005000000"
    "000000000000000000000000000000000000000000000000"
    "0000000000002440000000000000244000000000000024400000000000002440"
    "00000000000000000000000000000000000000000000000000"
    '\tAlpha\t{"OID": 1, "N2K_Site_N": "Alpha Site"}\t2026-01-01 00:00:00+00\n'
    f"{uuid4()}\t1\t"
    "0103000020346C00000100000005000000"
    "000000000000000000000000000000000000000000000000"
    "0000000000002440000000000000244000000000000024400000000000002440"
    "00000000000000000000000000000000000000000000000000"
    '\tBeta\t{"OID": 2, "N2K_Site_N": "Beta Site"}\t2026-01-01 00:00:00+00\n'
    "\\.\n"
)


@pytest.fixture
def s3_localstack(monkeypatch):
    endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4568")
    region = AWSConfig().region
    monkeypatch.setenv("DATA_SYNC_S3_BUCKET", BUCKET)
    monkeypatch.setenv("DATA_SYNC_S3_PREFIX", "dumps")
    monkeypatch.setenv("AWS_ENDPOINT_URL", endpoint)
    client = boto3.client("s3", region_name=region, endpoint_url=endpoint)
    with contextlib.suppress(client.exceptions.BucketAlreadyOwnedByYou):
        client.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": region},
        )
    return client


@pytest.fixture
def single_table_allow_list(monkeypatch):
    """Restrict the data-sync allow-list to just `nn_catchments`.

    Subset syncs are allowed (a manifest may name any subset of the allow-list),
    so narrowing the allow-list here just keeps these single-table tests focused
    on load / version-bump / gzip-rejection without other tables in play.
    """
    monkeypatch.setenv("DATA_SYNC_TABLES", '["nn_catchments"]')


@pytest.fixture
def two_table_allow_list(monkeypatch):
    """Restrict the data-sync allow-list to `nn_catchments` + `lpa_boundaries`
    for the tests that exercise two-table batches (atomicity, subset).
    """
    monkeypatch.setenv("DATA_SYNC_TABLES", '["nn_catchments", "lpa_boundaries"]')


def _seed(client, version: str) -> Manifest:
    """Upload the dump and return the manifest the caller would POST."""
    key = f"public_nn_catchments_{version}.sql.gz"
    client.put_object(
        Bucket=BUCKET, Key=f"dumps/{key}", Body=gzip.compress(DUMP_SQL.encode())
    )
    return Manifest(tables={"nn_catchments": {"key": key, "version": version}})


def test_reload_loads_rows_and_records_run(
    test_engine, s3_localstack, monkeypatch, single_table_allow_list
):
    monkeypatch.setenv("DB_IAM_AUTHENTICATION", "false")
    monkeypatch.setenv("DB_DATABASE", "test_nrf_impact")
    manifest = _seed(s3_localstack, "20260603_120000")

    run_id = uuid4()
    with test_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO public.data_sync_run (id, status) VALUES (:id, 'running')"
            ),
            {"id": str(run_id)},
        )

    run_data_sync(run_id, manifest, force=False)

    with test_engine.connect() as conn:
        count = conn.execute(text("SELECT count(*) FROM public.nn_catchments")).scalar()
        run_status = conn.execute(
            text("SELECT status FROM public.data_sync_run WHERE id = :id"),
            {"id": str(run_id)},
        ).scalar()
        hist = conn.execute(
            text("SELECT count(*) FROM public.data_load_history WHERE run_id = :id"),
            {"id": str(run_id)},
        ).scalar()

    assert count == 2
    assert run_status == "success"
    assert hist == 1

    # cleanup
    with test_engine.begin() as conn:
        conn.execute(text("DELETE FROM public.data_load_history"))
        conn.execute(text("DELETE FROM public.data_sync_run"))
        conn.execute(text("TRUNCATE public.nn_catchments"))


def _seed_atomic_failure(client, version: str) -> Manifest:
    """A valid nn_catchments dump followed by a dump that always fails.

    The second dump targets a non-existent column, so psql aborts the shared
    transaction after the (otherwise valid) nn_catchments load — exercising the
    all-or-nothing rollback across tables.
    """
    nn_key = f"public_nn_catchments_{version}.sql.gz"
    bad_key = f"public_lpa_boundaries_{version}.sql.gz"
    client.put_object(
        Bucket=BUCKET, Key=f"dumps/{nn_key}", Body=gzip.compress(DUMP_SQL.encode())
    )
    bad_sql = "COPY public.lpa_boundaries (no_such_column) FROM stdin;\n\\.\n"
    client.put_object(
        Bucket=BUCKET, Key=f"dumps/{bad_key}", Body=gzip.compress(bad_sql.encode())
    )
    # Insertion order is preserved: nn_catchments loads first, then the failing
    # table aborts the shared transaction.
    return Manifest(
        tables={
            "nn_catchments": {"key": nn_key, "version": version},
            "lpa_boundaries": {"key": bad_key, "version": version},
        },
    )


def test_reload_is_atomic_across_tables(
    test_engine, s3_localstack, monkeypatch, two_table_allow_list
):
    """If any table in the batch fails, every table is rolled back."""
    monkeypatch.setenv("DB_IAM_AUTHENTICATION", "false")
    monkeypatch.setenv("DB_DATABASE", "test_nrf_impact")
    manifest = _seed_atomic_failure(s3_localstack, "20260604_120000")

    run_id = uuid4()
    with test_engine.begin() as conn:
        conn.execute(text("TRUNCATE public.nn_catchments"))
        conn.execute(
            text(
                "INSERT INTO public.data_sync_run (id, status) VALUES (:id, 'running')"
            ),
            {"id": str(run_id)},
        )

    run_data_sync(run_id, manifest, force=False)

    with test_engine.connect() as conn:
        nn_count = conn.execute(
            text("SELECT count(*) FROM public.nn_catchments")
        ).scalar()
        run_status = conn.execute(
            text("SELECT status FROM public.data_sync_run WHERE id = :id"),
            {"id": str(run_id)},
        ).scalar()
        hist_rows = conn.execute(
            text(
                "SELECT table_name, status, status_detail "
                "FROM public.data_load_history WHERE run_id = :id"
            ),
            {"id": str(run_id)},
        ).all()

    # nn_catchments was loaded then rolled back with the failing table; nothing
    # was promoted, but one failed DataLoadHistory row per *selected* table is
    # still written so the audit trail shows which table/rule blocked the load.
    assert nn_count == 0
    assert run_status == "failed"
    hist_by_table = {row.table_name: row for row in hist_rows}
    assert set(hist_by_table) == {"nn_catchments", "lpa_boundaries"}
    assert all(row.status == "failed" for row in hist_by_table.values())
    # This particular failure (a genuine SQL error, not a QC-rule violation) has
    # no `table=X rule=Y detail=...` line to parse, so every table gets the
    # generic "blocked by QC failure" detail rather than a per-table one.
    assert all(
        row.status_detail is not None and "blocked by QC failure" in row.status_detail
        for row in hist_by_table.values()
    )

    # cleanup
    with test_engine.begin() as conn:
        conn.execute(text("DELETE FROM public.data_load_history"))
        conn.execute(text("DELETE FROM public.data_sync_run"))
        conn.execute(text("TRUNCATE public.nn_catchments"))


def test_reload_rejects_non_gzip_dump(
    test_engine, s3_localstack, monkeypatch, single_table_allow_list
):
    """A dump object that is not gzip fails fast, before any table is touched."""
    monkeypatch.setenv("DB_IAM_AUTHENTICATION", "false")
    monkeypatch.setenv("DB_DATABASE", "test_nrf_impact")
    version = "20260604_130000"
    key = f"public_nn_catchments_{version}.sql.gz"
    # Plain (uncompressed) bytes despite the .gz name.
    s3_localstack.put_object(Bucket=BUCKET, Key=f"dumps/{key}", Body=DUMP_SQL.encode())
    manifest = Manifest(tables={"nn_catchments": {"key": key, "version": version}})

    run_id = uuid4()
    with test_engine.begin() as conn:
        conn.execute(text("TRUNCATE public.nn_catchments"))
        conn.execute(
            text(
                "INSERT INTO public.data_sync_run (id, status) VALUES (:id, 'running')"
            ),
            {"id": str(run_id)},
        )

    run_data_sync(run_id, manifest, force=False)

    with test_engine.connect() as conn:
        nn_count = conn.execute(
            text("SELECT count(*) FROM public.nn_catchments")
        ).scalar()
        row = conn.execute(
            text("SELECT status, error FROM public.data_sync_run WHERE id = :id"),
            {"id": str(run_id)},
        ).one()

    assert nn_count == 0
    assert row.status == "failed"
    assert "gzip" in (row.error or "")

    # cleanup
    with test_engine.begin() as conn:
        conn.execute(text("DELETE FROM public.data_load_history"))
        conn.execute(text("DELETE FROM public.data_sync_run"))
        conn.execute(text("TRUNCATE public.nn_catchments"))


def test_reload_bumps_version_and_removes_old(
    test_engine, s3_localstack, monkeypatch, single_table_allow_list
):
    """Reloads bump the version and cleanup keeps only the latest two (DM-4)."""
    monkeypatch.setenv("DB_IAM_AUTHENTICATION", "false")
    monkeypatch.setenv("DB_DATABASE", "test_nrf_impact")

    # Start from an empty table so the first load is version 1.
    with test_engine.begin() as conn:
        conn.execute(text("TRUNCATE public.nn_catchments"))

    def _run(version: str) -> None:
        manifest = _seed(s3_localstack, version)
        run_id = uuid4()
        with test_engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO public.data_sync_run (id, status) "
                    "VALUES (:id, 'running')"
                ),
                {"id": str(run_id)},
            )
        run_data_sync(run_id, manifest, force=False)

    _run("20260603_120000")
    with test_engine.connect() as conn:
        first = conn.execute(
            text("SELECT MAX(version), COUNT(*) FROM public.nn_catchments")
        ).one()
        first_ids = set(
            conn.execute(text("SELECT id FROM public.nn_catchments")).scalars()
        )
    assert first[0] == 1
    assert first[1] == 2

    _run("20260604_120000")
    with test_engine.connect() as conn:
        second = conn.execute(
            text("SELECT MAX(version), COUNT(*) FROM public.nn_catchments")
        ).one()
        version_1_rows = conn.execute(
            text("SELECT COUNT(*) FROM public.nn_catchments WHERE version = 1")
        ).scalar()
        second_ids = set(
            conn.execute(
                text("SELECT id FROM public.nn_catchments WHERE version = 2")
            ).scalars()
        )
    assert second[0] == 2  # version bumped
    assert second[1] == 4  # cleanup retains version 1 (MAX-1) and version 2 (MAX)
    assert version_1_rows == 2  # version-1 rows retained for rollback
    assert first_ids.isdisjoint(second_ids)  # ids regenerated on load

    _run("20260605_120000")
    with test_engine.connect() as conn:
        third = conn.execute(
            text("SELECT MAX(version), COUNT(*) FROM public.nn_catchments")
        ).one()
        old_rows = conn.execute(
            text("SELECT COUNT(*) FROM public.nn_catchments WHERE version < 2")
        ).scalar()
    assert third[0] == 3  # version bumped again
    assert third[1] == 4  # only versions 2 and 3 remain
    assert old_rows == 0  # version-1 rows now removed once version 3 lands

    # cleanup
    with test_engine.begin() as conn:
        conn.execute(text("DELETE FROM public.data_load_history"))
        conn.execute(text("DELETE FROM public.data_sync_run"))
        conn.execute(text("DELETE FROM public.data_active_version"))
        conn.execute(text("TRUNCATE public.nn_catchments"))


def test_two_reloads_retain_previous_version_rows(
    test_engine, s3_localstack, monkeypatch, single_table_allow_list
):
    """Retention keeps MAX(version) and MAX(version)-1 (DM-4), not latest-only."""
    monkeypatch.setenv("DB_IAM_AUTHENTICATION", "false")
    monkeypatch.setenv("DB_DATABASE", "test_nrf_impact")

    with test_engine.begin() as conn:
        conn.execute(text("TRUNCATE public.nn_catchments"))

    for version in ("20260701_120000", "20260701_130000"):
        manifest = _seed(s3_localstack, version)
        run_id = uuid4()
        with test_engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO public.data_sync_run (id, status) VALUES (:id, 'running')"
                ),
                {"id": str(run_id)},
            )
        run_data_sync(run_id, manifest, force=True)

    with test_engine.connect() as conn:
        versions = (
            conn.execute(
                text(
                    "SELECT DISTINCT version FROM public.nn_catchments ORDER BY version"
                )
            )
            .scalars()
            .all()
        )

    assert versions == [1, 2]  # both retained, not just the latest

    # cleanup
    with test_engine.begin() as conn:
        conn.execute(text("DELETE FROM public.data_load_history"))
        conn.execute(text("DELETE FROM public.data_sync_run"))
        conn.execute(text("DELETE FROM public.data_active_version"))
        conn.execute(text("TRUNCATE public.nn_catchments"))


def test_reload_promotes_active_version(
    test_engine, s3_localstack, monkeypatch, single_table_allow_list
):
    monkeypatch.setenv("DB_IAM_AUTHENTICATION", "false")
    monkeypatch.setenv("DB_DATABASE", "test_nrf_impact")

    with test_engine.begin() as conn:
        conn.execute(text("TRUNCATE public.nn_catchments"))
        conn.execute(text("DELETE FROM public.data_active_version"))

    manifest = _seed(s3_localstack, "20260701_140000")
    run_id = uuid4()
    with test_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO public.data_sync_run (id, status) VALUES (:id, 'running')"
            ),
            {"id": str(run_id)},
        )
    run_data_sync(run_id, manifest, force=False)

    with test_engine.connect() as conn:
        active = conn.execute(
            text(
                "SELECT active_version FROM public.data_active_version "
                "WHERE table_name = 'nn_catchments'"
            )
        ).scalar()

    assert active == 1

    # cleanup
    with test_engine.begin() as conn:
        conn.execute(text("DELETE FROM public.data_load_history"))
        conn.execute(text("DELETE FROM public.data_sync_run"))
        conn.execute(text("DELETE FROM public.data_active_version"))
        conn.execute(text("TRUNCATE public.nn_catchments"))


def _run_manifest(test_engine, manifest, *, force: bool = False):
    run_id = uuid4()
    with test_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO public.data_sync_run (id, status) VALUES (:id, 'running')"
            ),
            {"id": str(run_id)},
        )
    run_data_sync(run_id, manifest, force=force)
    return run_id


def test_subset_sync_leaves_other_tables_untouched(
    test_engine, s3_localstack, monkeypatch, two_table_allow_list
):
    """A manifest naming only nn_catchments loads it and records/promotes it,
    without touching lpa_boundaries (which is in the allow-list but absent from
    the manifest) — the old completeness gate would have rejected this."""
    monkeypatch.setenv("DB_IAM_AUTHENTICATION", "false")
    monkeypatch.setenv("DB_DATABASE", "test_nrf_impact")

    with test_engine.begin() as conn:
        conn.execute(text("TRUNCATE public.nn_catchments"))
        conn.execute(text("DELETE FROM public.data_active_version"))
        lpa_before = conn.execute(
            text("SELECT COALESCE(MAX(version), 0) FROM public.lpa_boundaries")
        ).scalar()

    run_id = _run_manifest(test_engine, _seed(s3_localstack, "20260710_120000"))

    with test_engine.connect() as conn:
        status = conn.execute(
            text("SELECT status FROM public.data_sync_run WHERE id = :id"),
            {"id": str(run_id)},
        ).scalar()
        nn_hist = conn.execute(
            text(
                "SELECT table_name, status, row_version FROM public.data_load_history "
                "WHERE run_id = :id"
            ),
            {"id": str(run_id)},
        ).all()
        nn_active = conn.execute(
            text(
                "SELECT active_version FROM public.data_active_version "
                "WHERE table_name = 'nn_catchments'"
            )
        ).scalar()
        lpa_after = conn.execute(
            text("SELECT COALESCE(MAX(version), 0) FROM public.lpa_boundaries")
        ).scalar()
        lpa_active = conn.execute(
            text(
                "SELECT active_version FROM public.data_active_version "
                "WHERE table_name = 'lpa_boundaries'"
            )
        ).scalar()

    assert status == "success"
    # Only nn_catchments was recorded, with its promoted integer row_version.
    assert [(r.table_name, r.status, r.row_version) for r in nn_hist] == [
        ("nn_catchments", "success", 1)
    ]
    assert nn_active == 1
    # lpa_boundaries was never in the manifest: no version bump, no pointer.
    assert lpa_after == lpa_before
    assert lpa_active is None

    # cleanup
    with test_engine.begin() as conn:
        conn.execute(text("DELETE FROM public.data_load_history"))
        conn.execute(text("DELETE FROM public.data_sync_run"))
        conn.execute(text("DELETE FROM public.data_active_version"))
        conn.execute(text("TRUNCATE public.nn_catchments"))


def test_resync_same_version_is_noop(
    test_engine, s3_localstack, monkeypatch, single_table_allow_list
):
    """Re-posting a manifest whose table is already at that version loads
    nothing (no new integer version, no new history row) and still succeeds."""
    monkeypatch.setenv("DB_IAM_AUTHENTICATION", "false")
    monkeypatch.setenv("DB_DATABASE", "test_nrf_impact")

    with test_engine.begin() as conn:
        conn.execute(text("TRUNCATE public.nn_catchments"))
        conn.execute(text("DELETE FROM public.data_active_version"))

    manifest = _seed(s3_localstack, "20260711_120000")
    _run_manifest(test_engine, manifest)
    second_run = _run_manifest(test_engine, manifest)

    with test_engine.connect() as conn:
        max_version = conn.execute(
            text("SELECT MAX(version) FROM public.nn_catchments")
        ).scalar()
        second_hist = conn.execute(
            text("SELECT COUNT(*) FROM public.data_load_history WHERE run_id = :id"),
            {"id": str(second_run)},
        ).scalar()
        second_status = conn.execute(
            text("SELECT status FROM public.data_sync_run WHERE id = :id"),
            {"id": str(second_run)},
        ).scalar()

    assert max_version == 1  # no second load, version not bumped
    assert second_hist == 0  # no-op wrote no history row
    assert second_status == "success"

    # cleanup
    with test_engine.begin() as conn:
        conn.execute(text("DELETE FROM public.data_load_history"))
        conn.execute(text("DELETE FROM public.data_sync_run"))
        conn.execute(text("DELETE FROM public.data_active_version"))
        conn.execute(text("TRUNCATE public.nn_catchments"))


def _put_split_dump(client, base_key: str, sql: str, parts: int) -> list[str]:
    """Upload `sql` gzipped and sliced into `parts` objects, exactly as
    `pg_dump | gzip | split -b` produces: byte slices of ONE gzip member, so
    only the first part is a valid gzip file on its own.
    """
    blob = gzip.compress(sql.encode())
    size = len(blob) // parts + 1
    keys = []
    for i in range(0, len(blob), size):
        suffix = f"a{chr(ord('a') + i // size)}"
        key = f"{base_key}.part-{suffix}"
        client.put_object(Bucket=BUCKET, Key=f"dumps/{key}", Body=blob[i : i + size])
        keys.append(key)
    return keys


def test_split_dump_loads_identically_to_single_object(
    test_engine, s3_localstack, monkeypatch, single_table_allow_list
):
    """The core guarantee: a 3-part dump produces the same rows, active version,
    and history row as the same data loaded as one object.
    """
    monkeypatch.setenv("DB_IAM_AUTHENTICATION", "false")
    monkeypatch.setenv("DB_DATABASE", "test_nrf_impact")
    with test_engine.begin() as conn:
        conn.execute(text("TRUNCATE public.nn_catchments"))
        conn.execute(text("DELETE FROM public.data_active_version"))

    keys = _put_split_dump(s3_localstack, "split/nn.sql.gz", DUMP_SQL, parts=3)
    assert len(keys) == 3

    manifest = Manifest(
        tables={"nn_catchments": {"key": keys, "version": "20260727_split"}}
    )
    run_id = _run_manifest(test_engine, manifest)

    with test_engine.connect() as conn:
        count = conn.execute(text("SELECT count(*) FROM public.nn_catchments")).scalar()
        names = (
            conn.execute(text("SELECT name FROM public.nn_catchments ORDER BY name"))
            .scalars()
            .all()
        )
        active = conn.execute(
            text(
                "SELECT active_version FROM public.data_active_version "
                "WHERE table_name = 'nn_catchments'"
            )
        ).scalar()
        hist = conn.execute(
            text(
                "SELECT s3_key, etag, status, data_version, row_version "
                "FROM public.data_load_history WHERE run_id = :id"
            ),
            {"id": str(run_id)},
        ).one()

    assert count == 2
    assert names == ["Alpha", "Beta"]
    assert active == 1
    assert hist.status == "success"
    assert hist.data_version == "20260727_split"
    assert hist.row_version == 1
    # Base key + part count, not a raw list or a single part key.
    assert hist.s3_key == "split/nn.sql.gz [3 parts]"
    assert len(hist.etag) == 32

    # cleanup
    with test_engine.begin() as conn:
        conn.execute(text("DELETE FROM public.data_load_history"))
        conn.execute(text("DELETE FROM public.data_sync_run"))
        conn.execute(text("DELETE FROM public.data_active_version"))
        conn.execute(text("TRUNCATE public.nn_catchments"))


def test_missing_middle_part_fails_the_run_and_promotes_nothing(
    test_engine, s3_localstack, monkeypatch, single_table_allow_list
):
    """A part missing in S3 aborts before psql is spawned, leaving the live
    table and its active-version pointer exactly as they were.
    """
    monkeypatch.setenv("DB_IAM_AUTHENTICATION", "false")
    monkeypatch.setenv("DB_DATABASE", "test_nrf_impact")
    with test_engine.begin() as conn:
        conn.execute(text("TRUNCATE public.nn_catchments"))
        conn.execute(text("DELETE FROM public.data_active_version"))

    # Seed a good single-object load first, so there is state to preserve.
    _run_manifest(test_engine, _seed(s3_localstack, "20260727_good"))
    with test_engine.connect() as conn:
        before = conn.execute(
            text(
                "SELECT active_version FROM public.data_active_version "
                "WHERE table_name = 'nn_catchments'"
            )
        ).scalar()
        rows_before = conn.execute(
            text("SELECT count(*) FROM public.nn_catchments")
        ).scalar()

    keys = _put_split_dump(s3_localstack, "gap/nn.sql.gz", DUMP_SQL, parts=3)
    s3_localstack.delete_object(Bucket=BUCKET, Key=f"dumps/{keys[1]}")

    manifest = Manifest(
        tables={"nn_catchments": {"key": keys, "version": "20260727_gap"}}
    )
    # run_data_sync is a background job: it records the failure on the run row
    # rather than raising to the caller.
    gap_run_id = _run_manifest(test_engine, manifest, force=True)

    with test_engine.connect() as conn:
        run_status, run_error = conn.execute(
            text("SELECT status, error FROM public.data_sync_run WHERE id = :id"),
            {"id": str(gap_run_id)},
        ).one()
        after = conn.execute(
            text(
                "SELECT active_version FROM public.data_active_version "
                "WHERE table_name = 'nn_catchments'"
            )
        ).scalar()
        rows_after = conn.execute(
            text("SELECT count(*) FROM public.nn_catchments")
        ).scalar()

    assert run_status == "failed"
    assert "nn.sql.gz.part-ab" in run_error  # names the missing part
    assert after == before
    assert rows_after == rows_before

    # cleanup
    with test_engine.begin() as conn:
        conn.execute(text("DELETE FROM public.data_load_history"))
        conn.execute(text("DELETE FROM public.data_sync_run"))
        conn.execute(text("DELETE FROM public.data_active_version"))
        conn.execute(text("TRUNCATE public.nn_catchments"))


def test_custom_part_keys_sharing_a_basename_do_not_collide(
    test_engine, s3_localstack, monkeypatch, single_table_allow_list
):
    """Part keys are only required to be ordered, not to have distinct
    basenames. `chunk-1/data.gz` and `chunk-2/data.gz` must download to
    distinct local paths — sharing one would make the restore read the last
    part N times and corrupt the gzip stream.
    """
    monkeypatch.setenv("DB_IAM_AUTHENTICATION", "false")
    monkeypatch.setenv("DB_DATABASE", "test_nrf_impact")
    with test_engine.begin() as conn:
        conn.execute(text("TRUNCATE public.nn_catchments"))
        conn.execute(text("DELETE FROM public.data_active_version"))

    blob = gzip.compress(DUMP_SQL.encode())
    size = len(blob) // 3 + 1
    keys = []
    for i, start in enumerate(range(0, len(blob), size)):
        key = f"chunk-{i + 1}/data.gz"  # identical basename, distinct prefix
        s3_localstack.put_object(
            Bucket=BUCKET, Key=f"dumps/{key}", Body=blob[start : start + size]
        )
        keys.append(key)
    assert len({k.rsplit("/", 1)[-1] for k in keys}) == 1  # all share a basename

    manifest = Manifest(
        tables={"nn_catchments": {"key": keys, "version": "20260728_chunks"}}
    )
    _run_manifest(test_engine, manifest, force=True)

    with test_engine.connect() as conn:
        names = (
            conn.execute(text("SELECT name FROM public.nn_catchments ORDER BY name"))
            .scalars()
            .all()
        )

    assert names == ["Alpha", "Beta"]

    # cleanup
    with test_engine.begin() as conn:
        conn.execute(text("DELETE FROM public.data_load_history"))
        conn.execute(text("DELETE FROM public.data_sync_run"))
        conn.execute(text("DELETE FROM public.data_active_version"))
        conn.execute(text("TRUNCATE public.nn_catchments"))


def test_two_tables_sharing_a_dump_basename_do_not_collide(
    test_engine, s3_localstack, monkeypatch, two_table_allow_list
):
    """Different tables' dumps may share a basename under different prefixes.
    Each table downloads into its own directory, so a/data.gz and b/data.gz
    cannot overwrite each other before the restore reads them.
    """
    monkeypatch.setenv("DB_IAM_AUTHENTICATION", "false")
    monkeypatch.setenv("DB_DATABASE", "test_nrf_impact")
    with test_engine.begin() as conn:
        conn.execute(text("TRUNCATE public.nn_catchments"))
        conn.execute(text("TRUNCATE public.lpa_boundaries"))
        conn.execute(text("DELETE FROM public.data_active_version"))

    lpa_sql = (
        "COPY public.lpa_boundaries "
        "(id, version, geometry, name, attributes, created_at) FROM stdin;\n"
        f"{uuid4()}\t1\t"
        "0103000020346C00000100000005000000"
        "000000000000000000000000000000000000000000000000"
        "0000000000002440000000000000244000000000000024400000000000002440"
        "00000000000000000000000000000000000000000000000000"
        '\tAuthority A\t{"NAME": "Authority A"}\t2026-01-01 00:00:00+00\n'
        "\\.\n"
    )
    # Identical basename, different prefixes — one per table.
    s3_localstack.put_object(
        Bucket=BUCKET, Key="dumps/a/data.gz", Body=gzip.compress(DUMP_SQL.encode())
    )
    s3_localstack.put_object(
        Bucket=BUCKET, Key="dumps/b/data.gz", Body=gzip.compress(lpa_sql.encode())
    )

    manifest = Manifest(
        tables={
            "nn_catchments": {"key": "a/data.gz", "version": "20260728_collide"},
            "lpa_boundaries": {"key": "b/data.gz", "version": "20260728_collide"},
        }
    )
    _run_manifest(test_engine, manifest, force=True)

    with test_engine.connect() as conn:
        nn = conn.execute(text("SELECT count(*) FROM public.nn_catchments")).scalar()
        lpa = conn.execute(text("SELECT count(*) FROM public.lpa_boundaries")).scalar()

    assert nn == 2  # not lpa's single row, and not zero
    assert lpa == 1

    # cleanup
    with test_engine.begin() as conn:
        conn.execute(text("DELETE FROM public.data_load_history"))
        conn.execute(text("DELETE FROM public.data_sync_run"))
        conn.execute(text("DELETE FROM public.data_active_version"))
        conn.execute(text("TRUNCATE public.nn_catchments"))
        conn.execute(text("TRUNCATE public.lpa_boundaries"))
