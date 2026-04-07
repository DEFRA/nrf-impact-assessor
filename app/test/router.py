"""Test endpoints for local development.

These endpoints are only mounted when API_TESTING_ENABLED=true and are
never available in production.

Endpoints:
    POST /test/assess   - Run an assessment synchronously from a WKT geometry string.
                          No S3, no SQS, no polling — results returned immediately.
    POST /test/enqueue  - Convert WKT to GeoJSON and send an SQS job message with the
                          geometry embedded in the message body. The running consumer
                          processes it on its next poll, exercising the SQS pipeline
                          without requiring S3.
"""

import logging
import random
import time
from uuid import uuid4

import boto3
import geopandas as gpd
from botocore.exceptions import ClientError
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from shapely import wkt as shapely_wkt
from shapely.geometry import mapping
from sqlalchemy import func, select, text

from app.assess._geometry import inject_job_fields
from app.config import AWSConfig, DatabaseSettings
from app.models.db import CoefficientLayer, EdpBoundaryLayer, LookupTable, SpatialLayer
from app.models.enums import AssessmentType, SpatialLayerType
from app.models.job import BoundaryGeojson, ImpactAssessmentJob
from app.repositories.engine import create_db_engine
from app.repositories.repository import Repository
from app.runner.runner import run_assessment

logger = logging.getLogger(__name__)

_CRS_BNG = "EPSG:27700"

router = APIRouter()

# ---------------------------------------------------------------------------
# Lazy-initialised repository singleton
# ---------------------------------------------------------------------------
_repository: Repository | None = None


def _get_repository() -> Repository:
    """Get or create the module-level Repository singleton for test endpoints."""
    global _repository
    if _repository is None:
        logger.info("Initialising Repository for /test endpoints...")
        db_settings = DatabaseSettings()
        engine = create_db_engine(db_settings, pool_size=1, max_overflow=1)
        _repository = Repository(engine)
        logger.info("Repository initialised")
    return _repository


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _parse_assessment_type(value: str) -> AssessmentType:
    try:
        return AssessmentType(value)
    except ValueError as err:
        valid = [e.value for e in AssessmentType]
        raise HTTPException(
            status_code=400,
            detail=f"Invalid assessment_type '{value}'. Must be one of: {valid}",
        ) from err


def _wkt_to_gdf(wkt_str: str, crs: str) -> gpd.GeoDataFrame:
    """Parse a WKT string into a GeoDataFrame reprojected to EPSG:27700."""
    try:
        geometry = shapely_wkt.loads(wkt_str)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid WKT: {e}") from e

    gdf = gpd.GeoDataFrame(geometry=[geometry], crs=crs)

    if gdf.crs and gdf.crs.to_epsg() != 27700:
        gdf = gdf.to_crs(_CRS_BNG)

    return gdf


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class DbTableStatus(BaseModel):
    """Status of a single database table."""

    table: str
    row_count: int | None = None
    status: str
    error: str | None = None


class DbCheckResponse(BaseModel):
    """Response from GET /test/db."""

    db_connected: bool
    tables: list[DbTableStatus]


class WktAssessRequest(BaseModel):
    """Request body for POST /test/assess."""

    wkt: str
    crs: str = _CRS_BNG
    assessment_type: str = "nutrient"
    dwelling_type: str = "house"
    dwellings: int = 1
    name: str = ""


class WktAssessResponse(BaseModel):
    """Response from POST /test/assess."""

    job_id: str
    assessment_type: str
    timing_s: float
    results: dict


class WktEnqueueRequest(BaseModel):
    """Request body for POST /test/enqueue."""

    wkt: str
    crs: str = _CRS_BNG
    assessment_type: str = "nutrient"
    dwelling_type: str = "house"
    dwellings: int = 1
    name: str = ""
    developer_email: str = "test@example.com"


class WktEnqueueResponse(BaseModel):
    """Response from POST /test/enqueue."""

    job_id: str
    message_id: str
    note: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/db", responses={503: {"description": "Database not reachable"}})
def check_db() -> DbCheckResponse:
    """Check database connectivity and row counts for all reference tables.

    Returns a summary of each table's status and how many rows it contains.
    Spatial layer counts are broken down by layer_type.
    """
    repository = _get_repository()
    tables: list[DbTableStatus] = []

    # 1. Check DB connectivity
    try:
        with repository.session() as session:
            session.execute(text("SELECT 1"))
        db_connected = True
    except Exception as e:
        return DbCheckResponse(
            db_connected=False,
            tables=[
                DbTableStatus(table="db", row_count=None, status="error", error=str(e))
            ],
        )

    # 2. Count each table
    def _count(model) -> tuple[int | None, str, str | None]:
        try:
            with repository.session() as session:
                n = session.scalar(select(func.count()).select_from(model))
            status = "ok" if n and n > 0 else "empty"
            return n, status, None
        except Exception as e:
            return None, "error", str(e)

    for model, label in [
        (CoefficientLayer, "coefficient_layer"),
        (EdpBoundaryLayer, "edp_boundary_layer"),
        (LookupTable, "lookup_table"),
    ]:
        n, status, err = _count(model)
        tables.append(DbTableStatus(table=label, row_count=n, status=status, error=err))

    # 3. spatial_layer broken down by layer_type
    try:
        with repository.session() as session:
            rows = session.execute(
                select(SpatialLayer.layer_type, func.count().label("n")).group_by(
                    SpatialLayer.layer_type
                )
            ).all()

        loaded_types = {row.layer_type: row.n for row in rows}
        for layer_type in SpatialLayerType:
            n = loaded_types.get(layer_type, 0)
            status = "ok" if n > 0 else "empty"
            tables.append(
                DbTableStatus(
                    table=f"spatial_layer/{layer_type.value}",
                    row_count=n,
                    status=status,
                )
            )
    except Exception as e:
        tables.append(
            DbTableStatus(
                table="spatial_layer", row_count=None, status="error", error=str(e)
            )
        )

    return DbCheckResponse(db_connected=db_connected, tables=tables)


@router.post(
    "/assess",
    responses={
        400: {"description": "Invalid WKT or assessment_type"},
        500: {"description": "Assessment failed"},
    },
)
def assess_from_wkt(request: WktAssessRequest) -> WktAssessResponse:
    """Run an impact assessment synchronously from a WKT geometry string.

    Returns results immediately — no S3, no SQS, no polling needed.
    Useful for quickly testing assessment logic during local development.
    """
    assessment_type = _parse_assessment_type(request.assessment_type)
    gdf = _wkt_to_gdf(request.wkt, request.crs)

    job_id = str(uuid4())
    gdf = inject_job_fields(
        gdf, job_id, request.name, request.dwelling_type, request.dwellings
    )

    repository = _get_repository()

    start = time.time()
    try:
        dataframes = run_assessment(
            assessment_type=assessment_type.value,
            rlb_gdf=gdf,
            metadata={"unique_ref": job_id},
            repository=repository,
        )
    except (KeyError, ValueError) as e:
        raise HTTPException(status_code=500, detail=str(e)) from e

    timing_s = round(time.time() - start, 2)

    results = {}
    for key, df in dataframes.items():
        if hasattr(df, "geometry") and "geometry" in df.columns:
            results[key] = df.drop(columns=["geometry"]).to_dict(orient="records")
        else:
            results[key] = df.to_dict(orient="records")

    logger.info(
        "WKT assessment %s (%s) completed in %.2fs",
        job_id,
        assessment_type.value,
        timing_s,
    )

    return WktAssessResponse(
        job_id=job_id,
        assessment_type=assessment_type.value,
        timing_s=timing_s,
        results=results,
    )


@router.post(
    "/enqueue",
    status_code=202,
    responses={
        400: {"description": "Invalid WKT, assessment_type, or missing AWS config"},
        502: {"description": "LocalStack SQS not reachable"},
    },
)
def enqueue_to_sqs(request: WktEnqueueRequest) -> WktEnqueueResponse:
    """Enqueue an SQS job message with the geometry embedded in the message body.

    The running SQS consumer picks up the message on its next poll and processes it.
    The geometry is included directly in the message — no S3 upload required.

    Requires LocalStack running with SQS queue provisioned, and the consumer running
    (docker-compose worker profile or `python -m app.consumer`).
    """
    # Validate assessment_type even though only NUTRIENT is supported end-to-end,
    # so bad values return 400 rather than silently flowing through.
    _parse_assessment_type(request.assessment_type)
    aws = AWSConfig()

    if not aws.sqs_queue_url:
        raise HTTPException(
            status_code=400,
            detail="AWS_SQS_QUEUE_URL is not configured. Set it to the LocalStack queue URL.",
        )

    gdf = _wkt_to_gdf(request.wkt, request.crs)

    # Build an SNS-shaped quote payload. The reference must match ^NRF-\d{6}$.
    reference = f"NRF-{random.randint(0, 999_999):06d}"  # noqa: S311 - test tooling
    geometry_dict = mapping(gdf.geometry.iloc[0])

    client_kwargs: dict = {"region_name": aws.region}
    if aws.endpoint_url:
        client_kwargs["endpoint_url"] = aws.endpoint_url

    sqs = boto3.client("sqs", **client_kwargs)

    job = ImpactAssessmentJob(
        reference=reference,
        boundary_geojson=BoundaryGeojson(
            boundary_geometry_original=geometry_dict,
            intersecting_edps=[],
        ),
        development_types=[request.dwelling_type],
        residential_building_count=request.dwellings,
        email=request.developer_email,
    )

    try:
        logger.info("Sending job message to SQS queue: %s", aws.sqs_queue_url)
        response = sqs.send_message(
            QueueUrl=aws.sqs_queue_url,
            MessageBody=job.model_dump_json(by_alias=True),
        )
    except ClientError as e:
        logger.error("SQS send failed: %s", e)
        raise HTTPException(
            status_code=502,
            detail=f"SQS send failed — is LocalStack running? ({e})",
        ) from e

    message_id = response["MessageId"]
    logger.info("Enqueued job %s (SQS message ID: %s)", reference, message_id)

    return WktEnqueueResponse(
        job_id=reference,
        message_id=message_id,
        note="Consumer will process on next poll. Watch worker logs for: 'Processing job: "
        + reference
        + "'",
    )
