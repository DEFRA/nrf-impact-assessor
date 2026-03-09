"""Async assessment endpoints.

Production endpoints for submitting and polling impact assessments.
Jobs run in background threads via asyncio.to_thread() so the event loop
stays responsive for /health and other requests.

Endpoints:
    POST /assess          - Submit an assessment job (returns 202 with job_id)
    GET  /assess/{job_id} - Poll job status and retrieve results
"""

import asyncio
import logging
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated
from uuid import uuid4

from fastapi import APIRouter, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.assess._geometry import inject_job_fields, read_geometry_from_upload
from app.config import ApiServerConfig, DatabaseSettings
from app.repositories.engine import create_db_engine
from app.repositories.repository import Repository
from app.runner.runner import run_assessment

logger = logging.getLogger(__name__)

router = APIRouter()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
_config = ApiServerConfig()
_JOB_TTL_SECONDS = _config.assess_job_ttl_seconds
_MAX_JOBS = 100
_max_upload_bytes = 50 * 1024 * 1024  # 50 MB

# ---------------------------------------------------------------------------
# Job store
# ---------------------------------------------------------------------------


@dataclass
class JobState:
    status: str  # pending | running | completed | failed
    created_at: float = field(default_factory=time.time)
    results: dict | None = None
    error: str | None = None
    timing_s: float | None = None


_jobs: dict[str, JobState] = {}
_background_tasks: set[asyncio.Task] = set()

# ---------------------------------------------------------------------------
# Pydantic response models
# ---------------------------------------------------------------------------


class AssessSubmitResponse(BaseModel):
    job_id: str
    status: str
    poll_url: str


class AssessStatusResponse(BaseModel):
    job_id: str
    status: str
    results: dict | None = None
    error: str | None = None
    timing_s: float | None = None


# ---------------------------------------------------------------------------
# Lazy-initialised repository singleton
# ---------------------------------------------------------------------------
_repository: Repository | None = None


def _get_repository() -> Repository:
    """Get or create the module-level Repository singleton."""
    global _repository
    if _repository is None:
        logger.info("Initialising Repository for /assess endpoint...")
        db_settings = DatabaseSettings()
        engine = create_db_engine(db_settings, pool_size=2, max_overflow=2)
        _repository = Repository(engine)
        logger.info("Repository initialised")
    return _repository


# ---------------------------------------------------------------------------
# Housekeeping
# ---------------------------------------------------------------------------


def _prune_expired_jobs() -> None:
    """Remove jobs older than TTL to prevent unbounded memory growth."""
    now = time.time()
    expired = [
        jid for jid, state in _jobs.items() if now - state.created_at > _JOB_TTL_SECONDS
    ]
    for jid in expired:
        del _jobs[jid]
    if expired:
        logger.info("Pruned %d expired assessment jobs", len(expired))


# ---------------------------------------------------------------------------
# Background runner
# ---------------------------------------------------------------------------


def _run_assessment_sync(
    content: bytes,
    filename: str,
    assessment_type: str,
    job_id: str,
    name: str,
    dwelling_type: str,
    dwellings: int,
) -> dict:
    """Run an assessment synchronously (called from a thread)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        gdf = read_geometry_from_upload(content, filename, tmpdir_path)
        inject_job_fields(gdf, job_id, name, dwelling_type, dwellings)

        metadata = {"unique_ref": job_id}
        repository = _get_repository()

        dataframes = run_assessment(
            assessment_type=assessment_type,
            rlb_gdf=gdf,
            metadata=metadata,
            repository=repository,
        )

    # Convert DataFrames to JSON-serialisable dicts
    results = {}
    for key, df in dataframes.items():
        if hasattr(df, "geometry") and "geometry" in df.columns:
            results[key] = df.drop(columns=["geometry"]).to_dict(orient="records")
        else:
            results[key] = df.to_dict(orient="records")
    return results


async def _run_in_background(
    job_id: str,
    content: bytes,
    filename: str,
    assessment_type: str,
    name: str,
    dwelling_type: str,
    dwellings: int,
) -> None:
    """Async wrapper that offloads the assessment to a thread."""
    _jobs[job_id].status = "running"
    start = time.time()
    try:
        results = await asyncio.to_thread(
            _run_assessment_sync,
            content,
            filename,
            assessment_type,
            job_id,
            name,
            dwelling_type,
            dwellings,
        )
        _jobs[job_id].results = results
        _jobs[job_id].status = "completed"
        _jobs[job_id].timing_s = round(time.time() - start, 2)
        logger.info(
            "Assessment job %s completed in %.2fs", job_id, _jobs[job_id].timing_s
        )
    except Exception as e:
        _jobs[job_id].status = "failed"
        _jobs[job_id].error = str(e)
        _jobs[job_id].timing_s = round(time.time() - start, 2)
        logger.exception("Assessment job %s failed", job_id)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post(
    "/assess",
    response_model=AssessSubmitResponse,
    status_code=202,
    responses={
        400: {"description": "Invalid assessment_type"},
        413: {"description": "File too large (max 50 MB)"},
        503: {"description": "Server at job capacity"},
    },
)
async def submit_assessment(
    geometry_file: UploadFile,
    assessment_type: Annotated[str, Form()] = "nutrient",
    dwelling_type: Annotated[str, Form()] = "house",
    dwellings: Annotated[int, Form()] = 1,
    name: Annotated[str, Form()] = "Development",
):
    """Submit an impact assessment job for background processing.

    Returns immediately with a job_id. Poll GET /assess/{job_id} for results.
    """
    if assessment_type not in ("nutrient", "gcn"):
        raise HTTPException(
            status_code=400, detail=f"Invalid assessment_type: {assessment_type}"
        )

    if len(_jobs) >= _MAX_JOBS:
        _prune_expired_jobs()
        if len(_jobs) >= _MAX_JOBS:
            raise HTTPException(
                status_code=503,
                detail="Too many concurrent jobs. Try again later.",
            )

    _prune_expired_jobs()

    content = await geometry_file.read(_max_upload_bytes + 1)
    if len(content) > _max_upload_bytes:
        raise HTTPException(
            status_code=413, detail="File too large. Maximum upload size is 50 MB."
        )

    job_id = str(uuid4())
    filename = geometry_file.filename or "input.geojson"

    _jobs[job_id] = JobState(status="pending")

    task = asyncio.create_task(
        _run_in_background(
            job_id=job_id,
            content=content,
            filename=filename,
            assessment_type=assessment_type,
            name=name,
            dwelling_type=dwelling_type,
            dwellings=dwellings,
        )
    )
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)

    return JSONResponse(
        status_code=202,
        content=AssessSubmitResponse(
            job_id=job_id,
            status="pending",
            poll_url=f"/assess/{job_id}",
        ).model_dump(),
    )


@router.get(
    "/assess/{job_id}",
    response_model=AssessStatusResponse,
    responses={404: {"description": "Job not found"}},
)
async def get_assessment_status(job_id: str):
    """Poll the status of a submitted assessment job."""
    if job_id not in _jobs:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    state = _jobs[job_id]
    return AssessStatusResponse(
        job_id=job_id,
        status=state.status,
        results=state.results,
        error=state.error,
        timing_s=state.timing_s,
    )
