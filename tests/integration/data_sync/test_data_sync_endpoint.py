"""End-to-end data-sync reload against LocalStack S3 + test Postgres."""

import contextlib
import gzip
import json
import os
from uuid import uuid4

import boto3
import pytest
from sqlalchemy import text

from app.config import AWSConfig
from app.data_sync.service import run_data_sync

pytestmark = pytest.mark.integration

BUCKET = "nrf-ref-data-test"
# Minimal data-only dump: two rows into nn_catchments. restore_table drops and
# recreates whatever secondary indexes exist on the table during the load.
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


def _seed(client, version: str):
    dump_key = f"dumps/public_nn_catchments_{version}.sql.gz"
    client.put_object(
        Bucket=BUCKET, Key=dump_key, Body=gzip.compress(DUMP_SQL.encode())
    )
    manifest = {
        "data_version": version,
        "tables": {"nn_catchments": dump_key.removeprefix("dumps/")},
    }
    client.put_object(
        Bucket=BUCKET, Key="dumps/manifest.json", Body=json.dumps(manifest).encode()
    )


def test_reload_loads_rows_and_records_run(test_engine, s3_localstack, monkeypatch):
    monkeypatch.setenv("DB_IAM_AUTHENTICATION", "false")
    monkeypatch.setenv("DB_DATABASE", "test_nrf_impact")
    _seed(s3_localstack, "20260603_120000")

    run_id = uuid4()
    with test_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO public.data_sync_run (id, status) VALUES (:id, 'running')"
            ),
            {"id": str(run_id)},
        )

    run_data_sync(run_id, force=False)

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
