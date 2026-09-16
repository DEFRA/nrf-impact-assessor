"""Assessment Job Processor - coordinates geometry loading, validation, and assessment."""

import logging
import time
from datetime import UTC, datetime

import geopandas as gpd
import shapely
from shapely.geometry import shape

from app.assessments.adapters import nutrient_adapter
from app.assessments.reference_data import assert_reference_data_present
from app.boundary.catchments import find_intersecting_catchments
from app.calculators.levy import (
    LevyCalculation,
    LevyChargeUnavailableError,
    calculate_levy,
)
from app.clients.backend_client import BackendClient
from app.clients.payload_mapper import build_quote_patch_payload
from app.common.tracing import ctx_trace_id
from app.config import AWSConfig
from app.data_sync.service import resolve_active_provenance
from app.models.enums import AssessmentType
from app.models.job import ImpactAssessmentJob
from app.repositories.levy import (
    get_inflation_index,
    get_levy_charge,
    record_levy_calculation,
    resolve_edp_id,
)
from app.repositories.repository import Repository
from app.runner.runner import run_assessment

logger = logging.getLogger(__name__)


_NO_UNITS = "no housing units on job"
_NO_EDP_MATCH = "boundary intersects no EDP"
_MULTIPLE_EDPS = "boundary intersects multiple EDPs"
_NO_EDP_ID = "no EDP_id for label in edp_boundary_layer"
_NO_CHARGE = "no charge for EDP on date in levy_charges"
_NO_INFLATION_INDEX = "no RICS CIL index for edp start year or calculation year"


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
            LevyChargeUnavailableError: The levy could not be calculated (no
                housing units, zero or multiple intersecting EDPs, no EDP_id,
                or no charge for the date). Raised before anything else runs;
                the caller leaves the message on the queue.
            JobProcessingError: The job could not be completed (missing geometry,
                invalid geometry, or the assessment produced no results).
            EmptyReferenceDataError: A reference table the assessment needs is
                empty. Propagated so the caller leaves the message on the queue.
        """
        start_time = time.time()
        job_id = job.reference or "unknown"
        if job.trace_id:
            ctx_trace_id.set(job.trace_id)
        else:
            logger.warning(
                f"Job {job_id} has no trace_id; PATCH callback will omit the "
                "tracing header (message body missing 'traceId')"
            )
        logger.info(f"Job {job_id} started (assessment type: {assessment_type.value})")

        try:
            # The levy is the first step of the job (NRF2-913 decision 9): a
            # missing charge fails in milliseconds before the geometry check,
            # the reference-data guard and the multi-second spatial run, and a
            # redelivery does not repeat work it cannot finish. Raises
            # LevyChargeUnavailableError, which the consumer treats as "not
            # done": the message stays on the queue, no PATCH, so no quote
            # email (scenario 5).
            levy = self._calculate_levy(job)

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

            # Callback to nrf-backend if quote reference and EDPs are present
            self._send_results_callback(job, dataframes, levy)

            return dataframes

        except LevyChargeUnavailableError as e:
            # Named reason without a traceback, because the reason is the whole
            # story. The consumer logs its own line with the traceback when it
            # leaves the message on the queue, so this one stays terse.
            logger.error(f"Job {job_id} not started, levy unavailable: {e}")
            raise
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
        geojson_geom = job.boundary_geojson.boundary_geometry_original
        geom = shape(geojson_geom)
        if shapely.has_z(geom):
            # Reference layers are stored 2D (see scripts/load_data.py) and the
            # PostGIS temp tables use a 2D typmod, so drop any Z ordinate here.
            logger.info("Inline geometry has a Z dimension; flattening to 2D")
            geom = shapely.force_2d(geom)
        gdf = gpd.GeoDataFrame(geometry=[geom], crs="EPSG:27700")

        validation_errors = self._validate_geodataframe(gdf)
        if validation_errors:
            error_msg = "; ".join(validation_errors)
            msg = f"Geometry validation failed for job {job_id}: {error_msg}"
            raise JobProcessingError(msg)

        gdf = self._inject_job_data(gdf, job)

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
                (
                    f"Coordinates look like lon/lat degrees "
                    f"(bounds {minx:.6g}, {miny:.6g}, {maxx:.6g}, {maxy:.6g}); "
                    "expected EPSG:27700 (British National Grid) metres"
                )
            ]
        if not (minx >= 0 and maxx <= 700_000 and miny >= 0 and maxy <= 1_300_000):
            return [
                (
                    f"Coordinates outside EPSG:27700 bounds "
                    f"(bounds {minx:.6g}, {miny:.6g}, {maxx:.6g}, {maxy:.6g}); "
                    "expected easting 0-700000, northing 0-1300000"
                )
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

        return gdf

    def _calculate_levy(self, job: ImpactAssessmentJob) -> LevyCalculation | None:
        """Resolve the charge for the job's single EDP, calculate the levy and
        persist the scenario 7 audit row.

        Returns None only when there is no boundary at all: the geometry check
        that follows raises its own error for that case. A boundary that
        intersects zero or several EDPs is a levy failure like a missing
        charge, not a silent no-levy success (scenario 5): the calculation
        date can't be resolved to a single charge, so it fails closed.

        Raises:
            LevyChargeUnavailableError: no housing units on the job, the
                boundary intersects zero or multiple EDPs, no EDP_id for the
                label in the active boundary layer, no levy_charges row for
                that id covering today's date, or (when today falls in a
                different charging year than the EDP's publication) no RICS
                CIL index for one of those years.
        """
        job_id = job.reference or "unknown"
        calculation_date = datetime.now(UTC).date()

        def unavailable(reason: str, where: str) -> LevyChargeUnavailableError:
            msg = (
                f"Levy unavailable for quote {job_id} ({where}, "
                f"date={calculation_date}): {reason}"
            )
            return LevyChargeUnavailableError(msg)

        if not job.boundary_geojson:
            return None

        edps = job.boundary_geojson.intersecting_edps
        if len(edps) == 0:
            raise unavailable(_NO_EDP_MATCH, "edps=[]")
        if len(edps) > 1:
            labels = ", ".join(e.label for e in edps)
            raise unavailable(_MULTIPLE_EDPS, f"edps=[{labels}]")

        label = edps[0].label
        units = job.residential_building_count
        if not units or units <= 0:
            raise unavailable(_NO_UNITS, f"edp={label!r}")

        with self.repository.session() as session:
            edp_id = resolve_edp_id(session, label)
            if edp_id is None:
                raise unavailable(_NO_EDP_ID, f"edp={label!r}")
            charge = get_levy_charge(session, edp_id, calculation_date)
            if charge is None:
                raise unavailable(_NO_CHARGE, f"edp={label!r}, edp_id={edp_id}")

            edp_start_year_index = None
            calculation_year_index = None
            if calculation_date.year != charge.edp_start_date.year:
                edp_start_year_index = get_inflation_index(
                    session, charge.edp_start_date.year
                )
                calculation_year_index = get_inflation_index(
                    session, calculation_date.year
                )
                if edp_start_year_index is None or calculation_year_index is None:
                    raise unavailable(
                        _NO_INFLATION_INDEX,
                        f"edp={label!r}, edp_id={edp_id}, "
                        f"edp_start_year={charge.edp_start_date.year}, "
                        f"calculation_year={calculation_date.year}",
                    )

            levy = calculate_levy(
                charge,
                units=units,
                calculation_date=calculation_date,
                edp_start_year_index=edp_start_year_index,
                calculation_year_index=calculation_year_index,
            )
            record_levy_calculation(session, job_id, levy)
            session.commit()

        # Scenario 7 audit record, also in the log so a quote can be traced
        # without a database query. One line, key=value.
        logger.info(
            "Levy calculated: "
            f"quote={job_id} edp_id={levy.edp_id} edp_name={levy.edp_name!r} "
            f"edp_start_date={levy.edp_start_date} "
            f"calculator_version={levy.calculator_version} "
            f"base_charge_per_unit={levy.base_charge_per_unit} "
            f"rounded_charge_per_unit={levy.rounded_charge_per_unit} "
            f"units={levy.units} calculation_date={levy.calculation_date} "
            f"provisional_amount={levy.provisional_amount} "
            f"inflation_adjusted_amount={levy.inflation_adjusted_amount}"
        )
        return levy

    def _boundary_catchments(self, job: ImpactAssessmentJob) -> list[dict]:
        """The NN catchments the job's boundary falls in.

        Failures are swallowed: the catchments decorate a callback whose reason
        to exist is the assessment, so losing them must not lose the results.
        """
        if not job.boundary_geojson:
            return []
        try:
            geom = shape(job.boundary_geojson.boundary_geometry_original)
            if shapely.has_z(geom):
                geom = shapely.force_2d(geom)
            gdf = gpd.GeoDataFrame(geometry=[geom], crs="EPSG:27700")
            return find_intersecting_catchments(gdf, self.repository)
        except Exception:
            logger.exception(
                f"Could not resolve catchments for quote {job.reference}; "
                "sending the callback without them"
            )
            return []

    def _send_results_callback(
        self,
        job: ImpactAssessmentJob,
        dataframes: dict,
        levy: LevyCalculation | None,
    ) -> None:
        """Send assessment results to nrf-backend via PATCH /quotes/{reference}.

        Only fires when all conditions are met:
        - backend_client is configured
        - job has a quote reference
        - a levy was calculated for a single EDP

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

            intersecting_edps = (
                job.boundary_geojson.intersecting_edps if job.boundary_geojson else []
            )
            # Recomputed, not taken off the job: the backend's copy was made
            # when the boundary was checked, which may be an older
            # nn_catchments version than the one just assessed against.
            catchments = self._boundary_catchments(job)
            payload = build_quote_patch_payload(
                results=results,
                intersecting_edps=intersecting_edps,
                catchments=catchments,
                levy=levy,
            )
            if not payload.get("edps"):
                logger.error(
                    f"Empty EDP payload for quote {job.reference}, "
                    "skipping PATCH callback"
                )
                return

            start = time.time()
            response = self.backend_client.patch_quote(job.reference, payload)
            edps = payload["edps"]
            edp_names = ", ".join(e["edpName"] for e in edps)
            logger.info(
                f"Sent assessment results to nrf-backend for quote {job.reference} "
                f"(HTTP {response.status_code} in {time.time() - start:.2f}s, "
                f"{len(edps)} EDP(s): {edp_names})"
            )
        except Exception:
            logger.exception(
                f"Failed to send results to nrf-backend for quote {job.reference}"
            )
