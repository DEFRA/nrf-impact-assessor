"""Admin endpoint to trigger and poll a reference-data reload."""

import logging
from uuid import UUID, uuid4

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.config import AWSConfig, DatabaseSettings, DataSyncConfig
from app.data_sync.active_version import rollback_table
from app.data_sync.manifest import Manifest
from app.data_sync.service import run_data_sync
from app.models.db import DataLoadHistory, DataRollbackEvent, DataSyncRun
from app.repositories.engine import create_db_engine
from app.repositories.repository import clear_spatial_caches

logger = logging.getLogger(__name__)

router = APIRouter()


class RunInProgressError(Exception):
    """Raised when a reload run is already running (partial unique index hit)."""


# Lazy-initialised module-level engine (same pattern as the endpoint routers).
# A per-request engine would open a fresh connection — and generate a fresh
# IAM auth token — on every status poll.
_engine: Engine | None = None


def _get_engine() -> Engine:
    global _engine
    if _engine is None:
        # pool_size=2: the rollback endpoint holds an advisory-lock connection
        # and a Session connection concurrently for the duration of the call
        # (see rollback_data_sync), so a single-connection pool would
        # self-deadlock waiting for a second connection that never frees up.
        _engine = create_db_engine(
            DatabaseSettings(), AWSConfig(), pool_size=2, max_overflow=0
        )
    return _engine


def require_token(x_data_sync_token: str | None = Header(default=None)) -> None:
    cfg = DataSyncConfig()
    if not cfg.auth_token or x_data_sync_token != cfg.auth_token:
        raise HTTPException(status_code=401, detail="invalid or missing token")


class RollbackRequest(BaseModel):
    tables: list[str] | None = None


def _last_run_tables(session: Session) -> list[str]:
    """Distinct tables loaded by the most recent successful reload."""
    run = (
        session.query(DataSyncRun)
        .filter(DataSyncRun.status == "success", DataSyncRun.data_version.isnot(None))
        .order_by(DataSyncRun.started_at.desc())
        .first()
    )
    if run is None:
        return []
    rows = (
        session.query(DataLoadHistory.table_name)
        .filter(
            DataLoadHistory.run_id == run.id,
            DataLoadHistory.status.in_(["success", "reconciled"]),
        )
        .distinct()
        .all()
    )
    return [r[0] for r in rows]


def _create_run(*, forced: bool) -> UUID:
    """Insert a 'running' run row; raise RunInProgressError on contention."""
    run_id = uuid4()
    with Session(bind=_get_engine()) as session:
        session.add(DataSyncRun(id=run_id, status="running", forced=forced))
        try:
            session.commit()
        except IntegrityError as exc:
            session.rollback()
            raise RunInProgressError from exc
    return run_id


@router.post(
    "/admin/data-sync",
    status_code=202,
    dependencies=[Depends(require_token)],
    responses={
        401: {"description": "Invalid or missing data-sync token"},
        409: {"description": "A reload run is already in progress"},
    },
)
def trigger_data_sync(
    manifest: Manifest, background: BackgroundTasks, force: bool = False
) -> dict:
    try:
        run_id = _create_run(forced=force)
    except RunInProgressError as exc:
        raise HTTPException(
            status_code=409, detail="a reload run is already in progress"
        ) from exc
    background.add_task(run_data_sync, run_id, manifest, force=force)
    return {"run_id": str(run_id), "status": "running"}


@router.get(
    "/admin/data-sync/{run_id}",
    dependencies=[Depends(require_token)],
    responses={
        401: {"description": "Invalid or missing data-sync token"},
        404: {"description": "Run not found"},
    },
)
def get_data_sync(run_id: UUID) -> dict:
    with Session(bind=_get_engine()) as session:
        run = session.get(DataSyncRun, run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="run not found")
        return {
            "run_id": str(run.id),
            "status": run.status,
            "data_version": run.data_version,
            "forced": run.forced,
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "finished_at": (run.finished_at.isoformat() if run.finished_at else None),
            "error": run.error,
        }


@router.post(
    "/admin/data-sync/rollback",
    dependencies=[Depends(require_token)],
    responses={
        400: {"description": "No tables to roll back, or an invalid table was named"},
        401: {"description": "Invalid or missing data-sync token"},
        409: {"description": "A reload run is currently in progress"},
    },
)
def rollback_data_sync(body: RollbackRequest | None = None) -> dict:
    cfg = DataSyncConfig()
    allowed = set(cfg.tables)

    # Share the same advisory lock a reload holds for its entire duration
    # (see app/data_sync/service.py::run_data_sync), so a reload can never
    # start (or be mid-flight) while a rollback is reading/mutating the
    # active-version pointer, and vice versa. The in-progress `running`-row
    # check below stays — it gives a clearer 409 than blocking on the lock —
    # but now runs *after* the lock is held, closing the check-then-act race.
    engine = _get_engine()
    rolled_back: dict[str, dict[str, int]] = {}
    skipped: dict[str, str] = {}
    with engine.connect() as lock_conn:
        lock_conn.execution_options(isolation_level="AUTOCOMMIT")
        lock_conn.execute(text("SELECT pg_advisory_lock(:k)"), {"k": cfg.lock_key})
        try:
            with Session(bind=engine) as session:
                running = (
                    session.query(DataSyncRun)
                    .filter(DataSyncRun.status == "running")
                    .first()
                )
                if running is not None:
                    raise HTTPException(
                        status_code=409,
                        detail="a reload run is currently in progress",
                    )

                tables = (
                    body.tables if body and body.tables else _last_run_tables(session)
                )
                if not tables:
                    raise HTTPException(
                        status_code=400, detail="no reference tables to roll back"
                    )
                invalid = [t for t in tables if t not in allowed]
                if invalid:
                    raise HTTPException(
                        status_code=400,
                        detail=f"not in the data-sync allow-list: {', '.join(invalid)}",
                    )

                for table in tables:
                    try:
                        from_v, to_v = rollback_table(session, table)
                    except ValueError as exc:
                        skipped[table] = str(exc)
                        continue
                    rolled_back[table] = {"from": from_v, "to": to_v}
                    session.add(
                        DataRollbackEvent(
                            id=uuid4(),
                            table_name=table,
                            from_version=from_v,
                            to_version=to_v,
                        )
                    )
                session.commit()
        finally:
            lock_conn.execute(
                text("SELECT pg_advisory_unlock(:k)"), {"k": cfg.lock_key}
            )

    if rolled_back:
        # Active versions changed; drop in-process spatial caches so the next
        # assessment re-reads rather than serving pre-rollback results (mirrors
        # the reload path in app/data_sync/service.py).
        clear_spatial_caches()

    return {"rolled_back": rolled_back, "skipped": skipped}
