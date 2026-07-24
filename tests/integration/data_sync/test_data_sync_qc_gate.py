"""End-to-end proof that the QC gate (DM-1) rolls back a bad manifest."""

import contextlib
import gzip
import os
from uuid import uuid4

import boto3
import pytest
from sqlalchemy import text

from app.config import AWSConfig
from app.data_sync.manifest import Manifest
from app.data_sync.qc_rules import load_qc_rules
from app.data_sync.service import run_data_sync

pytestmark = pytest.mark.integration

BUCKET = "nrf-ref-data-qc-test"

_ALL_TABLES = [
    "coefficient_layer",
    "edp_boundary_layer",
    "edp_edges",
    "edp_excluded_areas",
    "gcn_ponds",
    "gcn_risk_zones",
    "lookup_table",
    "lpa_boundaries",
    "nn_catchments",
    "subcatchments",
    "wwtw_catchments",
]

# The same 10m x 10m square in EPSG:27700, as EWKB hex, in each shape the QC
# rules can declare. Keyed by the names used in GeometryRule.expected_types.
_GOOD_GEOM_BY_TYPE = {
    "Polygon": (
        "0103000020346C0000"  # SRID 27700 Polygon
        "0100000005000000"  # 1 ring, 5 points
        "00000000000000000000000000000000"
        "00000000000000000000000000002440"
        "00000000000024400000000000002440"
        "00000000000024400000000000000000"
        "00000000000000000000000000000000"
    ),
    "MultiPolygon": (
        "0106000020346C000001000000"  # SRID 27700 MultiPolygon, 1 part
        "0103000000"  # part is a Polygon
        "0100000005000000"  # 1 ring, 5 points
        "00000000000000000000000000000000"
        "00000000000000000000000000002440"
        "00000000000024400000000000002440"
        "00000000000024400000000000000000"
        "00000000000000000000000000000000"
    ),
}

_QC_TABLE_RULES = load_qc_rules().tables


def _good_geom(table: str) -> str:
    """EWKB hex of a geometry type `table`'s QC rules accept.

    Derived from qc_rules.yaml rather than hardcoded: a dump built with the
    wrong shape silently trips the geometry_type rule, so pinning one literal
    type here would let a rule change quietly invalidate every fixture below.
    Tables with no geometry rule are not shape-checked; Polygon serves.
    """
    rules = _QC_TABLE_RULES.get(table)
    if rules is None or rules.geometry is None:
        return _GOOD_GEOM_BY_TYPE["Polygon"]
    return _GOOD_GEOM_BY_TYPE[rules.geometry.expected_types[0]]


def _dump(table: str, columns: str, rows: list[str]) -> bytes:
    body = f"COPY public.{table} ({columns}) FROM stdin;\n" + "".join(rows) + "\\.\n"
    return gzip.compress(body.encode())


def _good_dumps() -> dict[str, bytes]:
    """Minimal one-row, QC-passing dump for every allow-listed table."""
    dumps = {}
    attrs_by_table = {
        "wwtw_catchments": '{"WwTw_ID": 1}',
        "nn_catchments": '{"OID": 1, "N2K_Site_N": "Site A"}',
        "subcatchments": '{"OPCAT_NAME": "Catchment A"}',
        "edp_boundary_layer": '{"EDP_Name": "Broads SAC (Yare & Bure) & Wensum SAC"}',
        "lpa_boundaries": '{"NAME": "Authority A"}',
        "gcn_risk_zones": '{"RZ": "Green"}',
        "gcn_ponds": "{}",
        "edp_edges": "{}",
        "edp_excluded_areas": "{}",
    }
    for table, attrs in attrs_by_table.items():
        dumps[table] = _dump(
            table,
            "id, version, geometry, name, attributes, created_at",
            [
                (
                    f"{uuid4()}\t1\t{_good_geom(table)}\tName\t{attrs}"
                    "\t2026-01-01 00:00:00+00\n"
                )
            ],
        )
    dumps["coefficient_layer"] = _dump(
        "coefficient_layer",
        "id, version, geometry, crome_id, land_use_cat, nn_catchment, "
        "subcatchment, lu_curr_n_coeff, lu_curr_p_coeff, n_resi_coeff, "
        "p_resi_coeff, created_at",
        [
            (
                f"{uuid4()}\t1\t{_good_geom('coefficient_layer')}\tCROME1\tARABLE"
                "\tSite A\tCatchment A"
                "\t10\t0.5\t12\t1.2\t2026-01-01 00:00:00+00\n"
            )
        ],
    )
    dumps["lookup_table"] = _dump(
        "lookup_table",
        "id, name, version, data, schema, description, source, license, created_at",
        [
            (
                f"{uuid4()}\twwtw_lookup\t1\t"
                '[{"wwtw_code": "1", "wwtw_subcatchment": "Catchment A"}]'
                "\t\\N\t\\N\t\\N\t\\N\t2026-01-01 00:00:00+00\n"
            ),
            (
                f"{uuid4()}\trates_lookup\t1\t"
                '[{"nn_catchment": "Site A", "occupancy_rate": 2.4}]'
                "\t\\N\t\\N\t\\N\t\\N\t2026-01-01 00:00:00+00\n"
            ),
        ],
    )
    return dumps


def _reset_sync_state(engine) -> None:
    """Return the sync bookkeeping tables to their empty, pre-run state.

    A run that reaches promotion also writes data_active_version, which other
    data_sync tests insert into assuming it is empty — so a successful run has
    more to undo than a rolled-back one.
    """
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM public.data_load_history"))
        conn.execute(text("DELETE FROM public.data_rollback_event"))
        conn.execute(text("DELETE FROM public.data_sync_run"))
        conn.execute(text("DELETE FROM public.data_active_version"))
        for table in _ALL_TABLES:
            conn.execute(text(f"TRUNCATE public.{table} CASCADE"))


@pytest.fixture
def s3_localstack():
    endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4568")
    region = AWSConfig().region
    client = boto3.client("s3", region_name=region, endpoint_url=endpoint)
    with contextlib.suppress(client.exceptions.BucketAlreadyOwnedByYou):
        client.create_bucket(
            Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": region}
        )
    return client


def test_bad_manifest_rolls_back_every_table_and_records_per_table_detail(
    test_engine, s3_localstack, monkeypatch
):
    monkeypatch.setenv("DB_IAM_AUTHENTICATION", "false")
    monkeypatch.setenv("DB_DATABASE", "test_nrf_impact")
    monkeypatch.setenv("DATA_SYNC_S3_BUCKET", BUCKET)
    monkeypatch.setenv("DATA_SYNC_S3_PREFIX", "dumps")
    monkeypatch.setenv(
        "AWS_ENDPOINT_URL", os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4568")
    )

    dumps = _good_dumps()
    # Break nn_catchments: row-count floor will be satisfied (first load, no
    # previous version) but its business key is NULL -> rule 3 fails.
    dumps["nn_catchments"] = _dump(
        "nn_catchments",
        "id, version, geometry, name, attributes, created_at",
        [
            (
                f"{uuid4()}\t1\t{_good_geom('nn_catchments')}\tBad\t\\N"
                "\t2026-01-01 00:00:00+00\n"
            )
        ],
    )
    # Break coefficient_layer: coefficient out of range.
    dumps["coefficient_layer"] = _dump(
        "coefficient_layer",
        "id, version, geometry, crome_id, land_use_cat, nn_catchment, "
        "subcatchment, lu_curr_n_coeff, lu_curr_p_coeff, n_resi_coeff, "
        "p_resi_coeff, created_at",
        [
            (
                f"{uuid4()}\t1\t{_good_geom('coefficient_layer')}\tCROME1\tARABLE"
                "\tSite A\tCatchment A"
                "\t9999\t0.5\t12\t1.2\t2026-01-01 00:00:00+00\n"
            )
        ],
    )

    version = "20260701_120000"
    tables_map = {}
    for table, body in dumps.items():
        key = f"public_{table}_{version}.sql.gz"
        s3_localstack.put_object(Bucket=BUCKET, Key=f"dumps/{key}", Body=body)
        tables_map[table] = key
    manifest = Manifest(data_version=version, tables=tables_map)

    with test_engine.begin() as conn:
        for table in _ALL_TABLES:
            conn.execute(text(f"TRUNCATE public.{table} CASCADE"))

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
        run_row = conn.execute(
            text("SELECT status, error FROM public.data_sync_run WHERE id = :id"),
            {"id": str(run_id)},
        ).one()
        counts = {
            table: conn.execute(
                text(f"SELECT count(*) FROM public.{table}")  # noqa: S608
            ).scalar()
            for table in _ALL_TABLES
        }
        history = conn.execute(
            text(
                "SELECT table_name, status, status_detail FROM public.data_load_history "
                "WHERE run_id = :id"
            ),
            {"id": str(run_id)},
        ).all()

    assert run_row.status == "failed"
    assert "nn_catchments" in run_row.error
    assert "coefficient_layer" in run_row.error
    assert all(count == 0 for count in counts.values())  # nothing promoted

    history_by_table = {row.table_name: row for row in history}
    assert set(history_by_table) == set(_ALL_TABLES)
    assert history_by_table["nn_catchments"].status == "failed"
    assert "key_not_null" in history_by_table["nn_catchments"].status_detail
    assert history_by_table["coefficient_layer"].status == "failed"
    assert "coefficient_range" in history_by_table["coefficient_layer"].status_detail
    # A clean table caught in the same rolled-back batch gets the generic detail.
    assert history_by_table["wwtw_catchments"].status == "failed"
    assert "blocked by QC failure" in history_by_table["wwtw_catchments"].status_detail

    # cleanup
    with test_engine.begin() as conn:
        conn.execute(text("DELETE FROM public.data_load_history"))
        conn.execute(text("DELETE FROM public.data_sync_run"))
        for table in _ALL_TABLES:
            conn.execute(text(f"TRUNCATE public.{table} CASCADE"))


def test_good_manifest_passes_qc_and_promotes_every_table(
    test_engine, s3_localstack, monkeypatch
):
    """The counterpart to the rollback test: _good_dumps must actually pass.

    Without this, a dump that violates a rule for every table still satisfies
    the rollback test above (it only asserts the run failed), so the fixtures
    can drift out of agreement with qc_rules.yaml unnoticed.
    """
    monkeypatch.setenv("DB_IAM_AUTHENTICATION", "false")
    monkeypatch.setenv("DB_DATABASE", "test_nrf_impact")
    monkeypatch.setenv("DATA_SYNC_S3_BUCKET", BUCKET)
    monkeypatch.setenv("DATA_SYNC_S3_PREFIX", "dumps")
    monkeypatch.setenv(
        "AWS_ENDPOINT_URL", os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4568")
    )

    version = "20260701_130000"
    tables_map = {}
    for table, body in _good_dumps().items():
        key = f"public_{table}_{version}.sql.gz"
        s3_localstack.put_object(Bucket=BUCKET, Key=f"dumps/{key}", Body=body)
        tables_map[table] = key
    manifest = Manifest(data_version=version, tables=tables_map)

    _reset_sync_state(test_engine)

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
        run_row = conn.execute(
            text("SELECT status, error FROM public.data_sync_run WHERE id = :id"),
            {"id": str(run_id)},
        ).one()
        counts = {
            table: conn.execute(
                text(f"SELECT count(*) FROM public.{table}")  # noqa: S608
            ).scalar()
            for table in _ALL_TABLES
        }
        history = conn.execute(
            text(
                "SELECT table_name, status FROM public.data_load_history "
                "WHERE run_id = :id"
            ),
            {"id": str(run_id)},
        ).all()

    assert run_row.status == "success", run_row.error
    assert run_row.error is None
    # Every table promoted its row(s); lookup_table carries two.
    assert counts["lookup_table"] == 2
    assert all(counts[t] == 1 for t in _ALL_TABLES if t != "lookup_table"), counts
    assert {row.table_name for row in history} == set(_ALL_TABLES)
    assert all(row.status == "success" for row in history)

    _reset_sync_state(test_engine)
