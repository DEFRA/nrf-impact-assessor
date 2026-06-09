"""Orchestration for an S3-triggered reference-data reload run."""

import logging
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import boto3
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.aws.s3 import S3Client, S3ObjectError
from app.config import AWSConfig, DatabaseSettings, DataSyncConfig
from app.data_sync.manifest import Manifest
from app.data_sync.restore import old_version_cleanup_sql, restore_all_atomic
from app.models.db import DataLoadHistory, DataSyncRun
from app.repositories.engine import create_db_engine

logger = logging.getLogger(__name__)


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

        if not needs_reload(manifest, _last_applied_version(session), force):
            logger.info("data_version %s already applied; no-op", manifest.data_version)
            _finish(session, run, status="success")
            return

        _restore_all(session, s3, cfg, db, region, run_id, manifest)
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
