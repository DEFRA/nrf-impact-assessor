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

pytestmark = pytest.mark.integration

BUCKET = "nrf-ref-data-test"
# Minimal data-only dump: two rows into nn_catchments. The restore truncates
# the table and replays this COPY data; indexes are left untouched (Liquibase
# owns them).
DUMP_SQL = (
    "COPY public.nn_catchments (id, version, geometry, name, attributes, created_at) "
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

    `_restore_all` now enforces manifest completeness (rule 1): the manifest
    must contain every allow-listed table. These tests exercise a single-table
    manifest (load/version-bump/gzip-rejection), not rule 1, so the allow-list
    is narrowed to match the manifest under test.
    """
    monkeypatch.setenv("DATA_SYNC_TABLES", '["nn_catchments"]')


@pytest.fixture
def two_table_allow_list(monkeypatch):
    """Restrict the data-sync allow-list to `nn_catchments` + `lpa_boundaries`.

    `test_reload_is_atomic_across_tables` exercises table-level atomicity (not
    rule 1's manifest-completeness check), so the allow-list is narrowed to
    match its two-table manifest.
    """
    monkeypatch.setenv("DATA_SYNC_TABLES", '["nn_catchments", "lpa_boundaries"]')


def _seed(client, version: str) -> Manifest:
    """Upload the dump and return the manifest the caller would POST."""
    key = f"public_nn_catchments_{version}.sql.gz"
    client.put_object(
        Bucket=BUCKET, Key=f"dumps/{key}", Body=gzip.compress(DUMP_SQL.encode())
    )
    return Manifest(data_version=version, tables={"nn_catchments": key})


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
        data_version=version,
        tables={"nn_catchments": nn_key, "lpa_boundaries": bad_key},
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
    # was promoted, but one failed DataLoadHistory row per manifest table is
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
    manifest = Manifest(data_version=version, tables={"nn_catchments": key})

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
