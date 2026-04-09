"""Test endpoints for local development.

These endpoints are only mounted when API_TESTING_ENABLED=true and are
never available in production.

Endpoints:
    POST /test/assess        - Run an assessment synchronously from a WKT geometry string.
                                No S3, no SQS, no polling — results returned immediately.
    POST /test/enqueue       - Convert WKT to GeoJSON and send an SQS job message with the
                                geometry embedded in the message body. The running consumer
                                processes it on its next poll, exercising the SQS pipeline
                                without requiring S3.
    POST /test/patch-backend - Fire a PATCH /quotes/{reference} at nrf-backend via
                                BackendClient using a stub (or caller-supplied) payload.
                                Validates BACKEND_BASE_URL, network path, and backend route
                                prefix — no assessment, no DB, no SQS.
"""

import logging
import random
import re
import time
from uuid import uuid4

import boto3
import geopandas as gpd
import httpx
from botocore.exceptions import ClientError
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from shapely import wkt as shapely_wkt
from shapely.geometry import mapping
from sqlalchemy import func, select, text

from app.assess._geometry import inject_job_fields
from app.clients.backend_client import BackendClient
from app.clients.payload_mapper import build_quote_patch_payload
from app.config import AWSConfig, BackendConfig, DatabaseSettings
from app.models.db import CoefficientLayer, EdpBoundaryLayer, LookupTable, SpatialLayer
from app.models.domain import (
    Development,
    ImpactAssessmentResult,
    LandUseImpact,
    NutrientImpact,
    SpatialAssignment,
    WastewaterImpact,
)
from app.models.enums import AssessmentType, SpatialLayerType
from app.models.job import BoundaryGeojson, EdpInput, ImpactAssessmentJob, LevyRange
from app.repositories.engine import create_db_engine
from app.repositories.repository import Repository
from app.runner.runner import run_assessment

_NRF_REFERENCE_PATTERN = re.compile(r"^NRF-\d{6}$")

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
    # Cosmetic test identifier in a test-only endpoint (API_TESTING_ENABLED=true); not security-sensitive.
    reference = f"NRF-{random.randint(0, 999_999):06d}"  # noqa: S311 # NOSONAR
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


# ---------------------------------------------------------------------------
# /test/patch-backend — exercise the nrf-backend callback path in isolation
# ---------------------------------------------------------------------------


class PatchBackendRequest(BaseModel):
    """Request body for POST /test/patch-backend."""

    reference: str = "NRF-000001"
    payload: dict | None = None

    model_config = {
        "json_schema_extra": {
            "example": {
                "reference": "NRF-000001",
                "payload": None,
            }
        }
    }


class PatchBackendResponse(BaseModel):
    """Response from POST /test/patch-backend."""

    reference: str
    url: str
    status: str


def _build_stub_patch_payload() -> dict:
    """Build a stub PATCH payload via the real payload_mapper.

    Routes through build_quote_patch_payload with a fully-typed
    ImpactAssessmentResult so the complete domain model chain is exercised.
    """
    stub_result = ImpactAssessmentResult(
        rlb_id=1,
        development=Development(
            id="TEST-001",
            name="Test Development",
            dwelling_category="house",
            source="test",
            dwellings=1,
            area_m2=1000.0,
            area_ha=0.1,
        ),
        spatial=SpatialAssignment(
            wwtw_id=141,
            wwtw_name="Test WwTW",
            wwtw_subcatchment="Test Subcatchment",
            lpa_name="Test LPA",
            nn_catchment="Broads",
            dev_subcatchment="Test Dev Subcatchment",
            area_in_nn_catchment_ha=0.1,
        ),
        land_use=LandUseImpact(
            nitrogen_kg_yr=10.0,
            phosphorus_kg_yr=4.0,
            nitrogen_post_suds_kg_yr=9.0,
            phosphorus_post_suds_kg_yr=3.5,
        ),
        wastewater=WastewaterImpact(
            occupancy_rate=2.4,
            water_usage_L_per_person_day=110.0,
            daily_water_usage_L=264.0,
            nitrogen_conc_2025_2030_mg_L=25.0,
            phosphorus_conc_2025_2030_mg_L=5.0,
            nitrogen_conc_2030_onwards_mg_L=20.0,
            phosphorus_conc_2030_onwards_mg_L=4.0,
            nitrogen_temp_kg_yr=2.41,
            phosphorus_temp_kg_yr=0.48,
            nitrogen_perm_kg_yr=1.93,
            phosphorus_perm_kg_yr=0.39,
        ),
        total=NutrientImpact(
            nitrogen_total_kg_yr=12.34,
            phosphorus_total_kg_yr=5.67,
        ),
    )
    stub_edp = EdpInput(
        edp_id=1,
        edp_name="Test EDP",
        edp_type="NUTRIENT",
        levy_gbp=LevyRange(min=100.0, max=200.0),
    )
    return build_quote_patch_payload([stub_result], [stub_edp])


@router.post(
    "/patch-backend",
    responses={
        400: {"description": "Invalid reference or BACKEND_BASE_URL not configured"},
        502: {"description": "Backend PATCH failed (HTTP error or transport error)"},
    },
)
def patch_backend(request: PatchBackendRequest) -> PatchBackendResponse:
    """Fire a PATCH /quotes/{reference} at nrf-backend via BackendClient.

    Plumbing test only — does not hit the DB, run an assessment, or touch SQS.
    Use it to validate that BACKEND_BASE_URL is set correctly, the network path
    to nrf-backend is reachable, and the route prefix matches.

    If `payload` is omitted, a stub payload is generated via the real
    payload_mapper so the mapper is exercised too. Pass a payload explicitly to
    reproduce bad-payload scenarios (e.g. to see a 400 from nrf-backend).

    Returns 502 on any backend failure (HTTP status error or transport error)
    so the HTTP status reflects reality; the detail message contains the
    underlying error text for diagnosis.
    """
    if not _NRF_REFERENCE_PATTERN.match(request.reference):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid reference '{request.reference}'. Must match ^NRF-\\d{{6}}$",
        )

    backend_config = BackendConfig()
    if not backend_config.base_url:
        raise HTTPException(
            status_code=400,
            detail="BACKEND_BASE_URL is not configured.",
        )

    payload = (
        request.payload if request.payload is not None else _build_stub_patch_payload()
    )

    client = BackendClient(
        base_url=backend_config.base_url,
        timeout=backend_config.callback_timeout,
        max_retries=backend_config.callback_max_retries,
    )
    url = f"{client.base_url}/quotes/{request.reference}"
    logger.info(f"Test PATCH → {url}")

    try:
        client.patch_quote(request.reference, payload)
    except httpx.HTTPStatusError as e:
        logger.error(f"Test PATCH {url} failed: HTTP {e.response.status_code}")
        raise HTTPException(
            status_code=502,
            detail=f"Backend PATCH failed with HTTP {e.response.status_code}: {e.response.text}",
        ) from e
    except httpx.TransportError as e:
        logger.error(f"Test PATCH {url} failed: transport error: {e}")
        raise HTTPException(
            status_code=502,
            detail=f"Backend PATCH failed with transport error: {e}",
        ) from e

    return PatchBackendResponse(
        reference=request.reference,
        url=url,
        status="ok",
    )
