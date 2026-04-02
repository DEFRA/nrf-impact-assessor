"""Assessment Job Processor - coordinates geometry loading, validation, and assessment."""

import logging
import tempfile
import time
from pathlib import Path

import geopandas as gpd
from shapely.geometry import shape

from app.aws.s3 import S3Client
from app.config import AWSConfig
from app.models.enums import AssessmentType
from app.models.geometry import GeometryFormat
from app.models.job import ImpactAssessmentJob
from app.repositories.repository import Repository
from app.runner.runner import run_assessment
from app.validation.geometry import GeometryValidator

logger = logging.getLogger(__name__)


class JobOrchestrator:
    """Orchestrates complete job lifecycle: load geometry → validate → assess → log results."""

    def __init__(
        self,
        aws_config: AWSConfig,
        repository: Repository,
    ):
        self.aws_config = aws_config
        self.repository = repository

        # S3 client only needed for legacy file-based geometry
        self.s3_input = None
        if aws_config.s3_input_bucket:
            self.s3_input = S3Client(
                bucket_name=aws_config.s3_input_bucket,
                region=aws_config.region,
                endpoint_url=aws_config.endpoint_url,
            )

    def process_job(
        self, job: ImpactAssessmentJob, assessment_type: AssessmentType
    ) -> dict:
        """Process a single job end-to-end.

        Geometry source priority:
        1. Inline boundaryGeojson from SQS message (production path)
        2. S3 download via s3_input_key (legacy/test path)

        Args:
            job: Job message from SQS
            assessment_type: The type of assessment to run for this job.

        Returns:
            Dictionary of assessment result DataFrames, or empty dict if validation
            fails or an error occurs.
        """
        start_time = time.time()
        job_id = job.effective_id
        logger.info(
            f"Processing job {job_id} for assessment type: {assessment_type.value}"
        )

        try:
            if job.boundary_geojson:
                dataframes = self._process_inline_geometry(job, assessment_type)
            elif job.s3_input_key and self.s3_input:
                dataframes = self._process_s3_geometry(job, assessment_type)
            else:
                logger.error(
                    f"No geometry source for job {job_id}: "
                    "neither boundaryGeojson nor s3_input_key provided"
                )
                return {}

            if not dataframes:
                logger.error(f"Assessment produced no results for job {job_id}")
                return {}

            processing_time = time.time() - start_time
            logger.info(
                f"Job {job_id} completed successfully in {processing_time:.2f}s"
            )
            logger.info(
                f"Processed {len(dataframes)} result set(s): {list(dataframes.keys())}"
            )
            return dataframes

        except Exception as e:
            logger.exception(f"Job {job_id} failed with exception: {e}")
            return {}

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
        job_id = job.effective_id
        logger.info("Step 1: Loading geometry from SQS message")
        geojson_geom = job.boundary_geojson.boundary_geometry_original
        geom = shape(geojson_geom)
        gdf = gpd.GeoDataFrame(geometry=[geom], crs="EPSG:27700")

        logger.info("Step 2: Validating inline geometry")
        validation_errors = self._validate_geodataframe(gdf)
        if validation_errors:
            error_msg = "; ".join(validation_errors)
            logger.error(f"Geometry validation failed for job {job_id}: {error_msg}")
            return {}

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

    def _process_s3_geometry(
        self, job: ImpactAssessmentJob, assessment_type: AssessmentType
    ) -> dict:
        """Process geometry downloaded from S3 (legacy/test path).

        Args:
            job: Job with s3_input_key pointing to geometry file.
            assessment_type: The type of assessment to run.

        Returns:
            Dictionary of assessment result DataFrames, or empty dict if validation fails.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            logger.info("Step 1: Downloading geometry file from S3")
            geometry_path, geometry_format = self.s3_input.download_geometry_file(
                s3_key=job.s3_input_key, local_dir=tmpdir_path
            )

            if not geometry_path.exists():
                msg = f"S3Client returned non-existent path: {geometry_path}"
                raise RuntimeError(msg)

            return self._process_geometry_file(
                job, geometry_path, geometry_format, assessment_type
            )

    def _process_geometry_file(
        self,
        job: ImpactAssessmentJob,
        geometry_path: Path,
        geometry_format: GeometryFormat,
        assessment_type: AssessmentType,
    ) -> dict:
        """Process geometry file: validate, inject job data, and run assessment.

        Args:
            job: ImpactAssessmentJob with development data
            geometry_path: Path to local geometry file
            geometry_format: GeometryFormat (SHAPEFILE or GEOJSON)
            assessment_type: The type of assessment to run.

        Returns:
            Dictionary of assessment result DataFrames, or empty dict if validation fails.
        """
        job_id = job.effective_id
        logger.info(
            "Step 2: Validating geometry (geometry only - no embedded attributes)"
        )
        geometry_validator = GeometryValidator()
        validation_errors = geometry_validator.validate(geometry_path, geometry_format)

        if validation_errors:
            error_msg = "; ".join([e.message for e in validation_errors])
            logger.error(f"Geometry validation failed for job {job_id}: {error_msg}")
            logger.error(
                f"  Validation errors: {[e.message for e in validation_errors]}"
            )
            return {}

        logger.info("Step 3: Loading geometry and injecting job data")
        gdf = gpd.read_file(geometry_path)
        gdf = self._inject_job_data(gdf, job)

        logger.info(f"Step 4: Running {assessment_type.value} assessment via runner")
        metadata = {"unique_ref": job_id}

        dataframes = run_assessment(
            assessment_type=assessment_type.value,
            rlb_gdf=gdf,
            metadata=metadata,
            repository=self.repository,
        )

        return dataframes

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
        return errors

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
        job_id = job.effective_id
        dwelling_type = job.effective_dwelling_type
        dwellings = job.effective_dwellings

        gdf["id"] = job_id
        gdf["name"] = job.development_name
        gdf["dwelling_category"] = dwelling_type
        gdf["source"] = "web_submission"
        gdf["dwellings"] = dwellings
        gdf["area_m2"] = gdf.geometry.area
        logger.info(f"Injected job data: id: {job_id} {dwellings} {dwelling_type}")

        return gdf
