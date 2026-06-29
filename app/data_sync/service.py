"""Orchestration for an S3-triggered reference-data reload run."""

import logging
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import boto3
from sqlalchemy import func, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.aws.s3 import S3Client, S3ObjectError
from app.config import AWSConfig, DatabaseSettings, DataSyncConfig
from app.data_sync.manifest import Manifest
from app.data_sync.restore import old_version_cleanup_sql, restore_all_atomic
from app.models.db import (
    CoefficientLayer,
    DataLoadHistory,
    DataSyncRun,
    EdpBoundaryLayer,
    EdpEdges,
    GcnPonds,
    GcnRiskZones,
    LookupTable,
    LpaBoundaries,
    NnCatchments,
    Subcatchments,
    WwtwCatchments,
)
from app.repositories.engine import create_db_engine, get_shared_repository
from app.repositories.repository import clear_spatial_caches

logger = logging.getLogger(__name__)

# Same reference tables the /test/db check endpoint reports on.
_REFERENCE_TABLES = [
    (CoefficientLayer, "coefficient_layer"),
    (EdpBoundaryLayer, "edp_boundary_layer"),
    (LookupTable, "lookup_table"),
    (WwtwCatchments, "wwtw_catchments"),
    (LpaBoundaries, "lpa_boundaries"),
    (NnCatchments, "nn_catchments"),
    (Subcatchments, "subcatchments"),
    (GcnRiskZones, "gcn_risk_zones"),
    (GcnPonds, "gcn_ponds"),
    (EdpEdges, "edp_edges"),
]


def _log_table_status(session: Session, *, context: str = "Post-sync") -> None:
    """Log one line of per-table row counts so an empty reference table is
    visible in the logs. `context` labels the message (e.g. "Post-sync",
    "No-op sync", "Startup"). Never raises: callers run it best-effort, so a
    failed count must not fail the surrounding operation.
    """
    try:
        parts: list[str] = []
        empty: list[str] = []
        errors: list[str] = []
        for model, label in _REFERENCE_TABLES:
            try:
                n = session.scalar(select(func.count()).select_from(model))
            except Exception as exc:  # noqa: BLE001
                session.rollback()
                parts.append(f"{label}=error")
                errors.append(f"{label} ({exc})")
                continue
            parts.append(f"{label}={n}")
            if not n:
                empty.append(label)
        summary = f"{context} table status: " + " ".join(parts)
        if empty or errors:
            if empty:
                summary += " EMPTY: " + ", ".join(empty)
            if errors:
                summary += " ERROR: " + ", ".join(errors)
            logger.warning(summary)
        else:
            logger.info("%s — all tables have rows", summary)
    except Exception:  # noqa: BLE001
        logger.warning("post-sync table status check failed", exc_info=True)


def log_startup_table_status() -> None:
    """Log reference-table row counts at app startup so an empty reference
    table is visible even when no reload ever runs. Best-effort: never raises,
    so a count failure (or an unavailable engine) cannot block startup.
    """
    try:
        repository = get_shared_repository()
        with repository.session() as session:
            _log_table_status(session, context="Startup")
    except Exception:  # noqa: BLE001
        logger.warning("startup table status check failed", exc_info=True)


def needs_reload(manifest: Manifest, applied_version: str | None, force: bool) -> bool:
    """Decide whether the manifest version requires a reload."""
    if force:
        return True
    return manifest.data_version != applied_version


def _last_applied_version(session: Session) -> str | None:
    row = (
        session.query(DataSyncRun.data_version)
        .filter(DataSyncRun.status == "success", DataSyncRun.data_version.isnot(None))
        .order_by(DataSyncRun.started_at.desc())
        .first()
    )
    return row[0] if row else None


def _build_s3_client(cfg: DataSyncConfig, aws: AWSConfig) -> S3Client:
    boto = boto3.client("s3", region_name=aws.region, endpoint_url=aws.endpoint_url)
    return S3Client(boto, bucket=cfg.s3_bucket, prefix=cfg.s3_prefix)


def _restore_all(
    session: Session,
    s3: S3Client,
    cfg: DataSyncConfig,
    db: DatabaseSettings,
    region: str,
    run_id: UUID,
    manifest: Manifest,
) -> None:
    allowed = set(cfg.tables)
    with tempfile.TemporaryDirectory() as tmp:
        items: list[tuple[str, Path]] = []
        audit: list[tuple[str, str, str]] = []
        for table, key in manifest.tables.items():
            if table not in allowed:
                msg = f"manifest table {table!r} is not in the data-sync allow-list"
                raise ValueError(msg)
            dest = Path(tmp) / Path(key).name
            try:
                etag = s3.object_etag(key)
                s3.download_object(key, dest)
            except S3ObjectError as exc:
                msg = f"{exc} (table {table!r})"
                raise S3ObjectError(msg) from exc
            items.append((table, dest))
            audit.append((table, key, etag))

        # Single transaction across all tables: either every table is loaded or
        # none is, so the reference data never exposes a mixed-version state.
        #
        # NOTE: the data load commits on the psql subprocess's own connection,
        # then DataLoadHistory commits separately on this ORM session. The two
        # commits are not atomic: a crash between them leaves the new version
        # applied but unrecorded in DataLoadHistory. This is low severity — the
        # version scheme self-heals (the next reload supersedes it) and readers
        # always see MAX(version) — but DataLoadHistory may under-report what is
        # actually loaded. Treat it as an audit log, not the source of truth.
        restore_all_atomic(db, region, items)

        # Only reached once the restore transaction has committed.
        for table, key, etag in audit:
            session.add(
                DataLoadHistory(
                    id=uuid4(),
                    run_id=run_id,
                    table_name=table,
                    s3_key=key,
                    etag=etag,
                    data_version=manifest.data_version,
                    status="success",
                )
            )
        session.commit()

        # Cutover has committed; remove superseded versions (best-effort).
        _cleanup_old_versions(session, [table for table, _ in items])


def _reconcile_load_history(
    session: Session, run_id: UUID, manifest: Manifest
) -> list[str]:
    """Backfill DataLoadHistory rows for any live table version that has no
    corresponding history record.

    The data load commits on the psql subprocess connection and DataLoadHistory
    commits separately on the ORM session (see _restore_all), so a crash between
    the two leaves the new version live but unrecorded. This detects a live
    MAX(version) per manifest table with no matching history row at that
    data_version and backfills a row marked `status = 'reconciled'`. Best-effort:
    a per-table failure is logged and skipped. Returns the tables it backfilled.
    """
    backfilled: list[str] = []
    model_by_name = {label: model for model, label in _REFERENCE_TABLES}
    for table in manifest.tables:
        model = model_by_name.get(table)
        if model is None:
            continue
        try:
            live_version = session.scalar(select(func.max(model.version)))
            if live_version is None:
                continue
            history_rows = session.scalar(
                select(func.count())
                .select_from(DataLoadHistory)
                .where(
                    DataLoadHistory.table_name == table,
                    DataLoadHistory.data_version == manifest.data_version,
                )
            )
            if history_rows:
                continue
            session.add(
                DataLoadHistory(
                    id=uuid4(),
                    run_id=run_id,
                    table_name=table,
                    s3_key=manifest.tables[table],
                    etag="",
                    data_version=manifest.data_version,
                    status="reconciled",
                )
            )
            session.commit()
            backfilled.append(table)
        except Exception:  # noqa: BLE001
            session.rollback()
            logger.warning(
                "history reconciliation failed for table %s", table, exc_info=True
            )
    return backfilled


def _cleanup_old_versions(session: Session, tables: list[str]) -> None:
    """Delete superseded versions per table (keep-latest). Best-effort: a
    failure is logged and skipped, since stale rows are ignored by MAX(version)
    and removed on the next reload. Cutover has already committed by this point.
    """
    for table in tables:
        try:
            session.execute(text(old_version_cleanup_sql(table)))
            session.commit()
        except Exception:  # noqa: BLE001
            session.rollback()
            logger.warning(
                "old-version cleanup failed for table %s; will retry next reload",
                table,
                exc_info=True,
            )


def run_data_sync(run_id: UUID, manifest: Manifest, *, force: bool) -> None:
    """Execute a reload run end-to-end. Always updates the run row's status.

    The manifest (version + table->dump-key map) is supplied by the caller of
    POST /admin/data-sync rather than read from S3.
    """
    cfg = DataSyncConfig()
    aws = AWSConfig()
    db = DatabaseSettings()
    engine = create_db_engine(db, aws, pool_size=2, max_overflow=2)
    region = aws.region
    try:
        with engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT")
            conn.execute(text("SELECT pg_advisory_lock(:k)"), {"k": cfg.lock_key})
            try:
                _do_run(engine, cfg, aws, db, region, run_id, manifest, force=force)
            finally:
                conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": cfg.lock_key})
    finally:
        engine.dispose()


def _do_run(
    engine: Engine,
    cfg: DataSyncConfig,
    aws: AWSConfig,
    db: DatabaseSettings,
    region: str,
    run_id: UUID,
    manifest: Manifest,
    *,
    force: bool,
) -> None:
    session = Session(bind=engine)
    run = session.get(DataSyncRun, run_id)
    if run is None:
        # _create_run inserts the run row before this task is dispatched, so a
        # missing row is unexpected; guard so the except/_finish path below never
        # dereferences None (there is nothing to mark failed if it doesn't exist).
        session.close()
        msg = f"data sync run {run_id} not found"
        raise RuntimeError(msg)
    try:
        s3 = _build_s3_client(cfg, aws)
        run.data_version = manifest.data_version
        session.commit()

        reconciled = _reconcile_load_history(session, run_id, manifest)
        if reconciled:
            logger.warning(
                "reconciled missing DataLoadHistory rows for: %s",
                ", ".join(reconciled),
            )

        if not needs_reload(manifest, _last_applied_version(session), force):
            logger.info("data_version %s already applied; no-op", manifest.data_version)
            # No reload, but still surface an empty reference table: emptiness
            # persists across no-op runs and would otherwise never be logged.
            _log_table_status(session, context="No-op sync")
            _finish(session, run, status="success")
            return

        _restore_all(session, s3, cfg, db, region, run_id, manifest)
        _log_table_status(session)
        # Reference data just changed; drop in-process spatial caches so the
        # next assessment re-reads from the database rather than serving
        # pre-reload results until their TTL expires.
        clear_spatial_caches()
        _finish(session, run, status="success")
    except Exception as exc:
        logger.exception("data sync run %s failed", run_id)
        _finish(session, run, status="failed", error=str(exc))
    finally:
        session.close()


def _finish(
    session: Session, run: DataSyncRun, *, status: str, error: str | None = None
) -> None:
    run.status = status
    run.error = error
    run.finished_at = datetime.now(UTC)
    session.commit()
