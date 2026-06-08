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
    "\tAlpha\t\\N\t2026-01-01 00:00:00+00\n"
    f"{uuid4()}\t1\t"
    "0103000020346C00000100000005000000"
    "000000000000000000000000000000000000000000000000"
    "0000000000002440000000000000244000000000000024400000000000002440"
    "00000000000000000000000000000000000000000000000000"
    "\tBeta\t\\N\t2026-01-01 00:00:00+00\n"
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


def _seed(client, version: str) -> Manifest:
    """Upload the dump and return the manifest the caller would POST."""
    key = f"public_nn_catchments_{version}.sql.gz"
    client.put_object(
        Bucket=BUCKET, Key=f"dumps/{key}", Body=gzip.compress(DUMP_SQL.encode())
    )
    return Manifest(data_version=version, tables={"nn_catchments": key})


def test_reload_loads_rows_and_records_run(test_engine, s3_localstack, monkeypatch):
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


def test_reload_is_atomic_across_tables(test_engine, s3_localstack, monkeypatch):
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
        hist = conn.execute(
            text("SELECT count(*) FROM public.data_load_history WHERE run_id = :id"),
            {"id": str(run_id)},
        ).scalar()

    # nn_catchments was loaded then rolled back with the failing table; no audit
    # rows are written because the restore transaction never committed.
    assert nn_count == 0
    assert run_status == "failed"
    assert hist == 0

    # cleanup
    with test_engine.begin() as conn:
        conn.execute(text("DELETE FROM public.data_load_history"))
        conn.execute(text("DELETE FROM public.data_sync_run"))
        conn.execute(text("TRUNCATE public.nn_catchments"))


def test_reload_rejects_non_gzip_dump(test_engine, s3_localstack, monkeypatch):
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
