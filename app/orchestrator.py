"""Assessment Job Processor - coordinates geometry loading, validation, and assessment."""

import logging
import time

import geopandas as gpd
from shapely.geometry import shape

from app.assessments.adapters import nutrient_adapter
from app.assessments.reference_data import assert_reference_data_present
from app.clients.backend_client import BackendClient
from app.clients.payload_mapper import build_quote_patch_payload
from app.common.tracing import ctx_trace_id
from app.config import AWSConfig
from app.data_sync.service import resolve_active_provenance
from app.models.enums import AssessmentType
from app.models.job import ImpactAssessmentJob
from app.repositories.repository import Repository
from app.runner.runner import run_assessment

logger = logging.getLogger(__name__)


class JobProcessingError(RuntimeError):
    """Raised when a job cannot be completed (bad input, no results, etc.).

    The consumer treats any exception out of process_job as "not done" and
    leaves the SQS message on the queue for redelivery / DLQ.
    """


class JobOrchestrator:
    """Orchestrates complete job lifecycle: load geometry → validate → assess → log results."""

    def __init__(
        self,
        aws_config: AWSConfig,
        repository: Repository,
        backend_client: BackendClient | None = None,
    ):
        self.aws_config = aws_config
        self.repository = repository
        self.backend_client = backend_client

    def process_job(
        self, job: ImpactAssessmentJob, assessment_type: AssessmentType
    ) -> dict:
        """Process a single job end-to-end from an inline SQS message.

        Args:
            job: Job message from SQS (must include boundaryGeojson).
            assessment_type: The type of assessment to run for this job.

        Returns:
            Dictionary of assessment result DataFrames on success.

        Raises:
            JobProcessingError: The job could not be completed (missing geometry,
                invalid geometry, or the assessment produced no results).
            EmptyReferenceDataError: A reference table the assessment needs is
                empty. Propagated so the caller leaves the message on the queue.
        """
        start_time = time.time()
        job_id = job.reference or "unknown"
        if job.trace_id:
            ctx_trace_id.set(job.trace_id)
            logger.info(f"Job {job_id} trace_id present, propagating to callbacks")
        else:
            logger.warning(
                f"Job {job_id} has no trace_id; PATCH callback will omit the "
                "tracing header (message body missing 'traceId')"
            )
        logger.info(
            f"Processing job {job_id} for assessment type: {assessment_type.value}"
        )

        try:
            if not job.boundary_geojson:
                msg = (
                    f"No geometry source for job {job_id}: boundaryGeojson is required"
                )
                raise JobProcessingError(msg)

            # Fail loudly (rather than producing empty results) when a required
            # reference table is empty, so the message is retried, not deleted.
            assert_reference_data_present(self.repository, assessment_type.value)

            dataframes = self._process_inline_geometry(job, assessment_type)

            if not dataframes:
                msg = f"Assessment produced no results for job {job_id}"
                raise JobProcessingError(msg)

            processing_time = time.time() - start_time
            logger.info(
                f"Job {job_id} completed successfully in {processing_time:.2f}s"
            )
            logger.info(
                f"Processed {len(dataframes)} result set(s): {list(dataframes.keys())}"
            )

            # Callback to nrf-backend if quote reference and EDPs are present
            self._send_results_callback(job, dataframes)

            return dataframes

        except Exception:
            logger.exception(f"Job {job_id} failed with exception")
            raise

    def _process_inline_geometry(
        self, job: ImpactAssessmentJob, assessment_type: AssessmentType
    ) -> dict:
        """Process geometry from inline boundaryGeojson in SQS message.

        Args:
            job: Job with boundary_geojson containing GeoJSON geometry.
            assessment_type: The type of assessment to run.

        Returns:
            Dictionary of assessment result DataFrames, or empty dict if validation fails.
        """
        job_id = job.reference or "unknown"
        logger.info("Step 1: Loading geometry from SQS message")
        geojson_geom = job.boundary_geojson.boundary_geometry_original
        geom = shape(geojson_geom)
        gdf = gpd.GeoDataFrame(geometry=[geom], crs="EPSG:27700")

        logger.info("Step 2: Validating inline geometry")
        validation_errors = self._validate_geodataframe(gdf)
        if validation_errors:
            error_msg = "; ".join(validation_errors)
            msg = f"Geometry validation failed for job {job_id}: {error_msg}"
            raise JobProcessingError(msg)

        logger.info("Step 3: Injecting job data")
        gdf = self._inject_job_data(gdf, job)

        logger.info(f"Step 4: Running {assessment_type.value} assessment via runner")
        metadata = {"unique_ref": job_id}
        return run_assessment(
            assessment_type=assessment_type.value,
            rlb_gdf=gdf,
            metadata=metadata,
            repository=self.repository,
        )

    def _validate_geodataframe(self, gdf: gpd.GeoDataFrame) -> list[str]:
        """Validate an in-memory GeoDataFrame.

        Checks for empty data, null geometries, invalid geometries,
        and non-polygon geometry types.

        Args:
            gdf: GeoDataFrame to validate.

        Returns:
            List of error messages (empty if valid).
        """
        errors = []
        if gdf.empty:
            errors.append("GeoDataFrame is empty")
            return errors
        if gdf.geometry.isna().any():
            errors.append("Contains null geometries")
        invalid = ~gdf.geometry.is_valid
        if invalid.any():
            errors.append(f"{invalid.sum()} invalid geometries")
        bad_types = ~gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"])
        if bad_types.any():
            errors.append(
                f"Non-polygon geometries: {gdf.geometry.geom_type[bad_types].tolist()}"
            )
        if not gdf.geometry.isna().any():
            errors.extend(self._validate_bng_bounds(gdf))
        return errors

    @staticmethod
    def _validate_bng_bounds(gdf: gpd.GeoDataFrame) -> list[str]:
        """Sanity-check that coordinates plausibly are EPSG:27700 metres.

        A lon/lat polygon is geometrically valid but sits at the wrong scale,
        so it intersects no reference data and the assessment fails silently.
        Lon/lat is checked first because degree values also fall inside the
        BNG easting/northing range.
        """
        minx, miny, maxx, maxy = gdf.total_bounds
        if minx >= -180 and maxx <= 180 and miny >= -90 and maxy <= 90:
            return [
                f"Coordinates look like lon/lat degrees "
                f"(bounds {minx:.6g}, {miny:.6g}, {maxx:.6g}, {maxy:.6g}); "
                "expected EPSG:27700 (British National Grid) metres"
            ]
        if not (minx >= 0 and maxx <= 700_000 and miny >= 0 and maxy <= 1_300_000):
            return [
                f"Coordinates outside EPSG:27700 bounds "
                f"(bounds {minx:.6g}, {miny:.6g}, {maxx:.6g}, {maxy:.6g}); "
                "expected easting 0-700000, northing 0-1300000"
            ]
        return []

    def _inject_job_data(
        self, gdf: gpd.GeoDataFrame, job: ImpactAssessmentJob
    ) -> gpd.GeoDataFrame:
        """Inject job data from SQS message into GeoDataFrame.

        Works with both quote-based and legacy message formats using
        the job's effective_* properties.

        Args:
            gdf: GeoDataFrame loaded from geometry (contains only geometry)
            job: Job message from SQS with development data

        Returns:
            GeoDataFrame with job data injected into columns
        """
        job_id = job.reference or "unknown"
        dwelling_type = job.development_types[0] if job.development_types else "housing"
        dwellings = job.residential_building_count or 0

        gdf["id"] = job_id
        gdf["name"] = job.reference or ""
        gdf["dwelling_category"] = dwelling_type
        gdf["source"] = "web_submission"
        gdf["dwellings"] = dwellings
        gdf["shape_area"] = gdf.geometry.area
        logger.info(f"Injected job data: id: {job_id} {dwellings} {dwelling_type}")

        return gdf

    def _send_results_callback(
        self, job: ImpactAssessmentJob, dataframes: dict
    ) -> None:
        """Send assessment results to nrf-backend via PATCH /quotes/{reference}.

        Only fires when all conditions are met:
        - backend_client is configured
        - job has a quote reference

        Failures are logged but do not affect the job result.
        """
        if not self.backend_client:
            logger.error("Backend client not configured, skipping results callback")
            return
        if not job.reference:
            logger.error("Job has no reference, skipping results callback")
            return

        try:
            with self.repository.session() as session:
                provenance = resolve_active_provenance(session)
            domain_results = nutrient_adapter.to_domain_models(
                dataframes, provenance=provenance
            )
            results = domain_results["assessment_results"]
            if not results:
                logger.error(
                    f"No assessment results for quote {job.reference}, "
                    "cannot send PATCH callback"
                )
                return

            result = results[0]
            if not result.catchment_impacts:
                logger.error(
                    f"No NN catchment found for quote {job.reference}, "
                    "cannot derive EDP for PATCH callback"
                )
                return

            payload = build_quote_patch_payload(results=results)
            if not payload.get("edps"):
                logger.error(
                    f"Empty EDP payload for quote {job.reference}, "
                    "skipping PATCH callback"
                )
                return

            self.backend_client.patch_quote(job.reference, payload)
            edps = payload["edps"]
            edp_names = ", ".join(e["edpName"] for e in edps)
            logger.info(
                f"Sent assessment results to nrf-backend for quote {job.reference} "
                f"({len(edps)} EDP(s): {edp_names})"
            )
        except Exception:
            logger.exception(
                f"Failed to send results to nrf-backend for quote {job.reference}"
            )
