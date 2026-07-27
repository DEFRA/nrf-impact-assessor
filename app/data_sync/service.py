"""Orchestration for an S3-triggered reference-data reload run."""

import hashlib
import logging
import tempfile
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import boto3
from sqlalchemy import func, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.aws.s3 import S3Client, S3ObjectError
from app.config import AWSConfig, DatabaseSettings, DataSyncConfig
from app.data_sync.active_version import get_active_version
from app.data_sync.manifest import Manifest, TableEntry
from app.data_sync.qc import parse_qc_failures
from app.data_sync.qc_rules import load_qc_rules
from app.data_sync.restore import (
    RestoreItem,
    old_version_cleanup_sql,
    restore_all_atomic,
)
from app.models.db import (
    CoefficientLayer,
    DataLoadHistory,
    DataSyncRun,
    EdpBoundaryLayer,
    EdpEdges,
    EdpExcludedAreas,
    GcnPonds,
    GcnRiskZones,
    LookupTable,
    LpaBoundaries,
    NnCatchments,
    Subcatchments,
    WwtwCatchments,
)
from app.models.domain import DataProvenance, TableProvenance
from app.repositories.engine import create_db_engine, get_shared_repository
from app.repositories.repository import clear_spatial_caches

logger = logging.getLogger(__name__)

# Canonical reference-table registry; the /test/db check endpoint iterates it too.
REFERENCE_TABLES = [
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
    (EdpExcludedAreas, "edp_excluded_areas"),
]

_MODEL_BY_TABLE_NAME = {label: model for model, label in REFERENCE_TABLES}


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
        for model, label in REFERENCE_TABLES:
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


def active_applied_version(session: Session, table: str) -> str | None:
    """The data_version currently applied for `table`, or None.

    Keyed off the *active integer version*: the version reads use
    (`get_active_version`, i.e. the active-version pointer or MAX(version)) is
    joined back to the DataLoadHistory row recorded for that exact row_version.
    Since history + pointer are written atomically with the data load (see
    restore.post_sql), this reliably answers "which manifest version is live for
    this table", independent of any stale or fabricated audit row for a
    non-active version.
    """
    active_int = get_active_version(session, table)
    if active_int is None:
        return None
    return session.scalar(
        select(DataLoadHistory.data_version)
        .where(
            DataLoadHistory.table_name == table,
            DataLoadHistory.row_version == active_int,
            DataLoadHistory.status.in_(["success", "reconciled"]),
        )
        .order_by(DataLoadHistory.loaded_at.desc())
        .limit(1)
    )


def resolve_active_provenance(session: Session) -> DataProvenance:
    """Return per-table reference-data provenance.

    For each reference table, reports the `data_version` and run id of the
    history row at that table's *active* integer version (the same join
    `active_applied_version` uses). Because it keys off the active-version
    pointer, it is rollback-accurate: after a rollback it reflects the version
    reads actually see, not merely the last load. Tables with no applied data
    are omitted, so an all-empty database yields an empty map.
    """
    tables: dict[str, TableProvenance] = {}
    for _model, name in REFERENCE_TABLES:
        active_int = get_active_version(session, name)
        if active_int is None:
            continue
        row = session.execute(
            select(DataLoadHistory.data_version, DataLoadHistory.run_id)
            .where(
                DataLoadHistory.table_name == name,
                DataLoadHistory.row_version == active_int,
                DataLoadHistory.status.in_(["success", "reconciled"]),
            )
            .order_by(DataLoadHistory.loaded_at.desc())
            .limit(1)
        ).first()
        if row is None:
            continue
        tables[name] = TableProvenance(data_version=row[0], data_sync_run_id=row[1])
    return DataProvenance(tables=tables)


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
    *,
    force: bool,
) -> list[str]:
    """Restore the subset of manifest tables whose version differs from what is
    currently applied (all of them under `force`). Returns the tables loaded.

    History rows and the active-version pointer are written inside the atomic
    restore transaction (see restore.post_sql), so data, audit, and cutover
    commit or roll back together — no separate ORM commit, no reconcile.
    """
    allowed = set(cfg.tables)
    for table in manifest.tables:
        if table not in allowed:
            msg = f"manifest table {table!r} is not in the data-sync allow-list"
            raise ValueError(msg)

    selected: list[tuple[str, TableEntry]] = [
        (table, entry)
        for table, entry in manifest.tables.items()
        if force or active_applied_version(session, table) != entry.version
    ]
    if not selected:
        logger.info("all manifest tables already at requested version; no-op")
        _log_table_status(session, context="No-op sync")
        return []

    with tempfile.TemporaryDirectory() as tmp:
        items: list[RestoreItem] = []
        for table, entry in selected:
            dumps: list[Path] = []
            etags: list[str] = []
            # One directory per table: dump keys are only required to be
            # distinct as S3 keys, so two tables may legitimately use the same
            # basename under different prefixes (a/data.gz, b/data.gz) and would
            # otherwise download over each other. `table` is safe as a path
            # component — it is checked against the allow-list above.
            table_dir = Path(tmp) / table
            table_dir.mkdir()
            for key, dest_name in zip(
                entry.keys, part_dest_names(entry.keys), strict=True
            ):
                dest = table_dir / dest_name
                try:
                    etags.append(s3.object_etag(key))
                    s3.download_object(key, dest)
                except S3ObjectError as exc:
                    msg = f"{exc} (table {table!r})"
                    raise S3ObjectError(msg) from exc
                dumps.append(dest)
            if entry.is_split:
                logger.info("Downloaded %d parts for table %s", len(dumps), table)
            items.append(
                RestoreItem(
                    table=table,
                    dumps=dumps,
                    s3_key=recorded_key(entry),
                    etag=recorded_etag(etags),
                    data_version=entry.version,
                )
            )

        # QC needs each table's active version for two reasons:
        #
        # - A referential check reads an unstaged referenced table straight from
        #   public.<table>; pin that read to the table's active version so a
        #   subset sync validates against the live data, not a superseded or
        #   rolled-back retained version. Staged tables read from their staging
        #   table instead. The tables that need a version here are the ones the
        #   referential checks reference — derived from the QC rules, not the
        #   allow-list (which a narrower config may not cover).
        # - A staged table's row-count floor is measured against its active
        #   version, which after a rollback is NOT MAX(version): MAX is the
        #   version rolled back from, typically the bad load that prompted the
        #   rollback. Measuring the floor against it would let an equally-bad
        #   reload pass the threshold.
        qc_rules = load_qc_rules()
        staged = {item.table for item in items}
        referenced = {
            side.table
            for check in qc_rules.referential_checks
            for side in (check.from_, check.to)
        }
        active_versions = {
            table: get_active_version(session, table) for table in referenced | staged
        }

        # One transaction across the selected tables: data load, DataLoadHistory
        # row, and active-version promotion all commit or roll back together, so
        # reference data never exposes a mixed-version state and the audit log is
        # never out of step with the live data.
        try:
            restore_all_atomic(
                db,
                region,
                items,
                run_id,
                qc_rules=qc_rules,
                active_versions=active_versions,
            )
        except RuntimeError as exc:
            _record_failed_history(session, run_id, selected, str(exc))
            raise

        # Cutover has committed; remove superseded versions (best-effort).
        _cleanup_old_versions(session, [item.table for item in items])

    return [table for table, _ in selected]


def part_dest_names(keys: list[str]) -> list[str]:
    """Local filenames for a table's dump parts, unique per key.

    A single key keeps its bare basename, unchanged from before multi-part
    support. Several parts are prefixed with their index, because part keys are
    only required to be ordered — not to have distinct basenames. Custom
    schemes like `chunk-1/data.gz` and `chunk-2/data.gz` share a basename and
    would otherwise download over each other, leaving the restore to read the
    last part N times and corrupt the gzip stream with no error.
    """
    if len(keys) == 1:
        return [Path(keys[0]).name]
    return [f"{i:04d}_{Path(key).name}" for i, key in enumerate(keys)]


def recorded_key(entry: TableEntry) -> str:
    """The `data_load_history.s3_key` value for a manifest entry.

    A single-key entry records its key verbatim, so history rows written before
    multi-part support stay directly comparable. A split entry records the
    common base with a part count — `data_load_history` keeps one row per table
    load, and the individual part keys are reconstructible from the base.
    """
    if not entry.is_split:
        return entry.keys[0]
    first = entry.keys[0]
    base = first.rsplit(".part-", 1)[0] if ".part-" in first else first
    return f"{base} [{len(entry.keys)} parts]"


def recorded_etag(etags: list[str]) -> str:
    """The `data_load_history.etag` value for a manifest entry.

    One part: the raw S3 ETag, unchanged. Several parts: a deterministic,
    order-sensitive digest of the parts' ETags, so any part changing changes the
    recorded value. Provenance only — skip/force is keyed on `version`.
    """
    if len(etags) == 1:
        return etags[0]
    digest = hashlib.sha256("\n".join(etags).encode())
    return digest.hexdigest()[:32]


def _record_failed_history(
    session: Session,
    run_id: UUID,
    selected: list[tuple[str, TableEntry]],
    error: str,
) -> None:
    """Write one `DataLoadHistory` row per *selected* table after a failed
    restore, so the audit trail shows which table/rule blocked the load even
    though nothing was promoted (the whole batch rolled back together). Only the
    tables actually chosen for restore are recorded — tables the manifest named
    but which were skipped as already-applied never took part in the batch. A
    table can fail multiple independent rules in one run (QC collects every
    failure before raising) — all of them are joined into status_detail, not
    just the last one.
    """
    failures = parse_qc_failures(error)
    details_by_table: dict[str, list[str]] = defaultdict(list)
    for f in failures:
        details_by_table[f.table].append(f"{f.rule}: {f.detail}")
    generic_detail = "blocked by QC failure on other table(s) in the same batch"
    for table, entry in selected:
        detail = (
            "; ".join(details_by_table[table])
            if table in details_by_table
            else generic_detail
        )
        session.add(
            DataLoadHistory(
                id=uuid4(),
                run_id=run_id,
                table_name=table,
                s3_key=recorded_key(entry),
                etag="",
                data_version=entry.version,
                status="failed",
                status_detail=detail,
            )
        )
        if table in details_by_table:
            logger.warning("QC failure on table %s: %s", table, detail)
    try:
        session.commit()
    except Exception:  # noqa: BLE001
        # Best-effort, exactly like _cleanup_old_versions: the restore has
        # already failed and rolled back, and the caller is about to re-raise
        # the RuntimeError that says why. Letting a write failure here escape
        # would replace that diagnosis with an unrelated one (e.g. a missing
        # column when the app runs ahead of its Liquibase changelog) and leave
        # the session poisoned, so the run row could never be marked failed.
        session.rollback()
        logger.warning(
            "failed to record failure history for run %s", run_id, exc_info=True
        )


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
        # Audit label only: with per-table versions there is no single global
        # version, so summarise the batch as its distinct table versions.
        run.data_version = ",".join(
            sorted({entry.version for entry in manifest.tables.values()})
        )
        session.commit()

        loaded = _restore_all(
            session, s3, cfg, db, region, run_id, manifest, force=force
        )
        if loaded:
            _log_table_status(session)
            # Reference data just changed; drop in-process spatial caches so the
            # next assessment re-reads from the database rather than serving
            # pre-reload results until their TTL expires.
            clear_spatial_caches()
        _finish(session, run, status="success")
    except Exception as exc:
        logger.exception("data sync run %s failed", run_id)
        # The failure may have left the session mid-transaction (or with a
        # pending rollback after a flush error); clear it so the status update
        # below can commit. Without this, marking the run failed itself raises
        # and the run row stays "running" forever.
        session.rollback()
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
