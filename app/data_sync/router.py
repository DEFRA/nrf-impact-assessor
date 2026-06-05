"""Admin endpoint to trigger and poll a reference-data reload."""

import logging
from uuid import UUID, uuid4

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.config import AWSConfig, DatabaseSettings, DataSyncConfig
from app.data_sync.manifest import Manifest
from app.data_sync.service import run_data_sync
from app.models.db import DataSyncRun
from app.repositories.engine import create_db_engine

logger = logging.getLogger(__name__)

router = APIRouter()


class RunInProgressError(Exception):
    """Raised when a reload run is already running (partial unique index hit)."""


def require_token(x_data_sync_token: str | None = Header(default=None)) -> None:
    cfg = DataSyncConfig()
    if not cfg.auth_token or x_data_sync_token != cfg.auth_token:
        raise HTTPException(status_code=401, detail="invalid or missing token")


def _create_run(*, forced: bool) -> UUID:
    """Insert a 'running' run row; raise RunInProgressError on contention."""
    db = DatabaseSettings()
    engine = create_db_engine(db, AWSConfig(), pool_size=1, max_overflow=0)
    run_id = uuid4()
    try:
        with Session(bind=engine) as session:
            session.add(DataSyncRun(id=run_id, status="running", forced=forced))
            try:
                session.commit()
            except IntegrityError as exc:
                session.rollback()
                raise RunInProgressError from exc
    finally:
        engine.dispose()
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
    db = DatabaseSettings()
    engine = create_db_engine(db, AWSConfig(), pool_size=1, max_overflow=0)
    try:
        with Session(bind=engine) as session:
            run = session.get(DataSyncRun, run_id)
            if run is None:
                raise HTTPException(status_code=404, detail="run not found")
            return {
                "run_id": str(run.id),
                "status": run.status,
                "data_version": run.data_version,
                "forced": run.forced,
                "started_at": run.started_at.isoformat() if run.started_at else None,
                "finished_at": (
                    run.finished_at.isoformat() if run.finished_at else None
                ),
                "error": run.error,
            }
    finally:
        engine.dispose()
