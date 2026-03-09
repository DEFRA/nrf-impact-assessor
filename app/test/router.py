"""Test endpoints for local development.

These endpoints are only mounted when API_TESTING_ENABLED=true and are
never available in production.

Endpoints:
    POST /test/assess   - Run an assessment synchronously from a WKT geometry string.
                          No S3, no SQS, no polling — results returned immediately.
    POST /test/enqueue  - Convert WKT to GeoJSON, upload to LocalStack S3, and send
                          an SQS job message. The running consumer processes it on its
                          next poll, exercising the full production pipeline.
"""

import logging
import time
from datetime import UTC, datetime
from uuid import uuid4

import boto3
import geopandas as gpd
from botocore.exceptions import ClientError
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from shapely import wkt as shapely_wkt

from app.assess._geometry import inject_job_fields
from app.config import AWSConfig, DatabaseSettings
from app.models.enums import AssessmentType
from app.models.job import ImpactAssessmentJob
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
    s3_key: str
    message_id: str
    note: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


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
        502: {"description": "LocalStack S3/SQS not reachable"},
    },
)
def enqueue_to_sqs(request: WktEnqueueRequest) -> WktEnqueueResponse:
    """Upload a WKT geometry to LocalStack S3 and enqueue an SQS job message.

    The running SQS consumer picks up the message on its next poll and processes it
    through the full production pipeline (orchestrator → S3 download → validate →
    run_assessment). Use this to verify the end-to-end SQS code path locally.

    Requires LocalStack running with S3 bucket and SQS queue provisioned, and the
    consumer running (docker-compose worker profile or `python -m app.consumer`).
    """
    assessment_type = _parse_assessment_type(request.assessment_type)
    aws = AWSConfig()

    if not aws.s3_input_bucket:
        raise HTTPException(
            status_code=400,
            detail="AWS_S3_INPUT_BUCKET is not configured. Set it to the LocalStack bucket name.",
        )
    if not aws.sqs_queue_url:
        raise HTTPException(
            status_code=400,
            detail="AWS_SQS_QUEUE_URL is not configured. Set it to the LocalStack queue URL.",
        )

    gdf = _wkt_to_gdf(request.wkt, request.crs)

    job_id = str(uuid4())
    s3_key = f"jobs/{job_id}/input.geojson"

    geojson_bytes = gdf.to_json().encode()

    client_kwargs: dict = {"region_name": aws.region}
    if aws.endpoint_url:
        client_kwargs["endpoint_url"] = aws.endpoint_url

    s3 = boto3.client("s3", **client_kwargs)
    sqs = boto3.client("sqs", **client_kwargs)

    try:
        logger.info("Uploading GeoJSON to s3://%s/%s", aws.s3_input_bucket, s3_key)
        s3.put_object(
            Bucket=aws.s3_input_bucket,
            Key=s3_key,
            Body=geojson_bytes,
            **( {"ExpectedBucketOwner": aws.account_id} if aws.account_id else {} ),
        )
    except ClientError as e:
        logger.error("S3 upload failed: %s", e)
        raise HTTPException(
            status_code=502,
            detail=f"S3 upload failed — is LocalStack running? ({e})",
        ) from e

    job = ImpactAssessmentJob(
        job_id=job_id,
        s3_input_key=s3_key,
        developer_email=request.developer_email,
        submitted_at=datetime.now(UTC),
        assessment_type=assessment_type,
        dwelling_type=request.dwelling_type,
        number_of_dwellings=request.dwellings,
        development_name=request.name,
    )

    try:
        logger.info("Sending job message to SQS queue: %s", aws.sqs_queue_url)
        response = sqs.send_message(
            QueueUrl=aws.sqs_queue_url,
            MessageBody=job.model_dump_json(),
        )
    except ClientError as e:
        logger.error("SQS send failed: %s", e)
        raise HTTPException(
            status_code=502,
            detail=f"SQS send failed — is LocalStack running? ({e})",
        ) from e

    message_id = response["MessageId"]
    logger.info("Enqueued job %s (SQS message ID: %s)", job_id, message_id)

    return WktEnqueueResponse(
        job_id=job_id,
        s3_key=s3_key,
        message_id=message_id,
        note="Consumer will process on next poll. Watch worker logs for: 'Processing job: "
        + job_id
        + "'",
    )
