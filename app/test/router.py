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
from pydantic import BaseModel, Field
from shapely import wkt as shapely_wkt
from shapely.geometry import mapping
from sqlalchemy import func, select, text

from app.assess._geometry import inject_job_fields
from app.clients.backend_client import BackendClient
from app.clients.payload_mapper import build_quote_patch_payload
from app.common.tracing import ctx_trace_id
from app.config import AWSConfig, BackendConfig, DatabaseSettings
from app.models.db import (
    CoefficientLayer,
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
from app.models.domain import (
    CatchmentImpact,
    Development,
    ImpactAssessmentResult,
    LandUseImpact,
    NutrientImpact,
    SpatialAssignment,
    WastewaterImpact,
)
from app.models.enums import AssessmentType
from app.models.job import BoundaryGeojson, ImpactAssessmentJob
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
    trace_id: str | None = Field(
        default=None,
        description=(
            "CDP trace id to embed in the queued message body as `traceId`. "
            "When omitted, falls back to the inbound `x-cdp-request-id` header "
            "(set by TraceIdMiddleware). The consumer propagates this onto the "
            "outbound PATCH callback so the whole flow stays traced."
        ),
    )


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
        (WwtwCatchments, "wwtw_catchments"),
        (LpaBoundaries, "lpa_boundaries"),
        (NnCatchments, "nn_catchments"),
        (Subcatchments, "subcatchments"),
        (GcnRiskZones, "gcn_risk_zones"),
        (GcnPonds, "gcn_ponds"),
        (EdpEdges, "edp_edges"),
    ]:
        n, status, err = _count(model)
        tables.append(DbTableStatus(table=label, row_count=n, status=status, error=err))

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

    # Explicit body trace_id wins; otherwise inherit from inbound x-cdp-request-id
    # (TraceIdMiddleware sets ctx_trace_id for the duration of this request).
    # Trailing `or None` collapses empty strings to None so we never serialise
    # `"traceId": ""` into the queued message.
    trace_id = request.trace_id or ctx_trace_id.get(None) or None

    job = ImpactAssessmentJob(
        reference=reference,
        boundary_geojson=BoundaryGeojson(
            boundary_geometry_original=geometry_dict,
            intersecting_edps=[],
        ),
        development_types=[request.dwelling_type],
        residential_building_count=request.dwellings,
        trace_id=trace_id,
    )

    try:
        logger.info("Sending job message to SQS queue: %s", aws.sqs_queue_url)
        response = sqs.send_message(
            QueueUrl=aws.sqs_queue_url,
            MessageBody=job.model_dump_json(by_alias=True),
        )
    except ClientError as e:
        logger.exception("SQS send failed")
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
    stub_edps: int = Field(
        default=1,
        ge=1,
        le=10,
        description="Number of stub EDP catchments to generate when payload is omitted",
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "reference": "NRF-000001",
                "payload": None,
                "stub_edps": 1,
            }
        }
    }


class PatchBackendResponse(BaseModel):
    """Response from POST /test/patch-backend."""

    reference: str
    url: str
    status: str


_STUB_CATCHMENTS = [
    ("1", "Broads", 12.34, 5.67),
    ("2", "Wensum", 30.00, 8.10),
    ("3", "Norfolk Fens", 18.75, 3.22),
    ("4", "Stour", 9.50, 2.40),
    ("5", "Ant", 6.00, 1.80),
    ("6", "Yare", 14.20, 4.55),
    ("7", "Waveney", 22.60, 6.30),
    ("8", "Thurne", 5.40, 1.10),
    ("9", "Bure", 11.90, 3.70),
    ("10", "Chet", 3.80, 0.95),
]


def _build_stub_patch_payload(stub_edps: int = 1) -> dict:
    """Build a stub PATCH payload via the real payload_mapper."""
    catchments = [
        CatchmentImpact(
            catchment_id=cid,
            catchment_name=name,
            nitrogen_total_kg_yr=n,
            phosphorus_total_kg_yr=p,
        )
        for cid, name, n, p in _STUB_CATCHMENTS[:stub_edps]
    ]
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
        total=NutrientImpact(nitrogen_total_kg_yr=0.0, phosphorus_total_kg_yr=0.0),
        catchment_impacts=catchments,
    )
    return build_quote_patch_payload([stub_result])


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
        request.payload
        if request.payload is not None
        else _build_stub_patch_payload(request.stub_edps)
    )

    client = BackendClient(
        base_url=backend_config.base_url,
        timeout=backend_config.callback_timeout,
        max_retries=backend_config.callback_max_retries,
    )
    url = f"{client.base_url}/quotes/{request.reference}"
    logger.info("Test PATCH payload → %s payload=%s", url, payload)

    try:
        client.patch_quote(request.reference, payload)
    except httpx.HTTPStatusError as e:
        logger.exception(f"Test PATCH {url} failed: HTTP {e.response.status_code}")
        raise HTTPException(
            status_code=502,
            detail=f"Backend PATCH failed with HTTP {e.response.status_code}: {e.response.text}",
        ) from e
    except httpx.TransportError as e:
        logger.exception(f"Test PATCH {url} failed: transport error")
        raise HTTPException(
            status_code=502,
            detail=f"Backend PATCH failed with transport error: {e}",
        ) from e

    return PatchBackendResponse(
        reference=request.reference,
        url=url,
        status="ok",
    )
