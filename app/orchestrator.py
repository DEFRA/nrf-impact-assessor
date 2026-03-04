"""Assessment Job Processor - coordinates S3 download, validation, and assessment."""

import logging
import tempfile
import time
from pathlib import Path

import geopandas as gpd

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
    """Orchestrates complete job lifecycle: download → validate → assess → log results."""

    def __init__(
        self,
        aws_config: AWSConfig,
        repository: Repository,
    ):
        self.aws_config = aws_config
        self.repository = repository

        # Initialize S3 client (input only)
        self.s3_input = S3Client(
            bucket_name=aws_config.s3_input_bucket,
            region=aws_config.region,
            endpoint_url=aws_config.endpoint_url,
        )

    def process_job(
        self, job: ImpactAssessmentJob, assessment_type: AssessmentType
    ) -> dict:
        """Process a single job end-to-end.

        Pipeline:
        1. Download geometry file from S3
        2. Validate geometry
        3. Inject job data into GeoDataFrame
        4. Run impact assessment
        5. Log results

        Args:
            job: Job message from SQS
            assessment_type: The type of assessment to run for this job.

        Returns:
            Dictionary of assessment result DataFrames, or empty dict if validation
            fails or an error occurs.
        """
        start_time = time.time()
        logger.info(
            f"Processing job {job.job_id} for assessment type: {assessment_type.value}"
        )

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                tmpdir_path = Path(tmpdir)

                logger.info("Step 1: Downloading geometry file from S3")
                geometry_path, geometry_format = self.s3_input.download_geometry_file(
                    s3_key=job.s3_input_key, local_dir=tmpdir_path
                )

                if not geometry_path.exists():
                    msg = f"S3Client returned non-existent path: {geometry_path}"
                    raise RuntimeError(msg)

                # Process geometry file (steps 2-4)
                dataframes = self._process_geometry_file(
                    job,
                    geometry_path,
                    geometry_format,
                    assessment_type,
                )

                if not dataframes:
                    logger.error(f"Assessment produced no results for job {job.job_id}")
                    return {}

                processing_time = time.time() - start_time
                logger.info(
                    f"Job {job.job_id} completed successfully in {processing_time:.2f}s"
                )
                logger.info(
                    f"Processed {len(dataframes)} result set(s): {list(dataframes.keys())}"
                )
                return dataframes

        except Exception as e:
            logger.exception(f"Job {job.job_id} failed with exception: {e}")
            return {}

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
        logger.info(
            "Step 2: Validating geometry (geometry only - no embedded attributes)"
        )
        geometry_validator = GeometryValidator()
        validation_errors = geometry_validator.validate(geometry_path, geometry_format)

        if validation_errors:
            error_msg = "; ".join([e.message for e in validation_errors])
            logger.error(
                f"Geometry validation failed for job {job.job_id}: {error_msg}"
            )
            logger.error(
                f"  Validation errors: {[e.message for e in validation_errors]}"
            )
            return {}

        logger.info("Step 3: Loading geometry and injecting job data")
        gdf = gpd.read_file(geometry_path)
        gdf = self._inject_job_data(gdf, job)

        logger.info(f"Step 4: Running {assessment_type.value} assessment via runner")
        metadata = {"unique_ref": job.job_id}

        dataframes = run_assessment(
            assessment_type=assessment_type.value,
            rlb_gdf=gdf,
            metadata=metadata,
            repository=self.repository,
        )

        return dataframes

    def _inject_job_data(
        self, gdf: gpd.GeoDataFrame, job: ImpactAssessmentJob
    ) -> gpd.GeoDataFrame:
        """Inject job data from SQS message into GeoDataFrame.

        In production, geometry files contain only geometry. All development data
        comes from the frontend form via the SQS job message. This method injects
        that data into the GeoDataFrame so the assessment service can use it.

        Args:
            gdf: GeoDataFrame loaded from geometry file (contains only geometry)
            job: Job message from SQS with development data

        Returns:
            GeoDataFrame with job data injected into columns
        """
        gdf["id"] = job.job_id
        gdf["name"] = job.development_name
        gdf["dwelling_category"] = job.dwelling_type
        gdf["source"] = "web_submission"
        gdf["dwellings"] = job.number_of_dwellings
        gdf["area_m2"] = gdf.geometry.area
        logger.info(
            f"Injected job data: id: {job.job_id} {job.number_of_dwellings} {job.dwelling_type}"
        )

        return gdf
