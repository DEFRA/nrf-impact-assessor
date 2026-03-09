"""S3 operations for geometry file download."""

import logging
import zipfile
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from app.models.geometry import GeometryFormat

logger = logging.getLogger(__name__)


class S3Client:
    """Handles S3 operations for geometry file input (shapefile or GeoJSON)."""

    def __init__(self, bucket_name: str, region: str, endpoint_url: str | None = None):
        self.bucket_name = bucket_name
        self.region = region
        client_kwargs: dict = {"region_name": region}
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url
        self.s3 = boto3.client("s3", **client_kwargs)

    def download_geometry_file(
        self, s3_key: str, local_dir: Path
    ) -> tuple[Path, GeometryFormat]:
        """Download geometry file from S3 (shapefile zip or GeoJSON).

        Handles two formats:
        - Shapefile: Must be a ZIP file containing .shp and components
        - GeoJSON: Single .geojson or .json file

        Args:
            s3_key: S3 key to geometry file
                (e.g., "jobs/abc123/input.zip" or "jobs/abc123/input.geojson")
            local_dir: Directory to download/extract file

        Returns:
            Tuple of (path to geometry file, format type)

        Raises:
            ValueError: If file format is invalid or shapefile zip doesn't contain .shp
            ClientError: If S3 download fails
        """
        s3_key_lower = s3_key.lower()

        if s3_key_lower.endswith(".zip"):
            path = self._download_and_extract_shapefile_zip(s3_key, local_dir)
            return path, GeometryFormat.SHAPEFILE
        if s3_key_lower.endswith((".geojson", ".json")):
            path = self._download_geojson(s3_key, local_dir)
            return path, GeometryFormat.GEOJSON
        msg = f"Unsupported file format: {s3_key}. Expected .zip (shapefile), .geojson, or .json"
        raise ValueError(msg)

    def _download_and_extract_shapefile_zip(self, s3_key: str, local_dir: Path) -> Path:
        logger.info(f"Downloading shapefile ZIP from s3://{self.bucket_name}/{s3_key}")

        zip_path = local_dir / "input.zip"
        try:
            self.s3.download_file(self.bucket_name, s3_key, str(zip_path))
        except ClientError as e:
            logger.error(f"Failed to download from S3: {e}")
            raise

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(local_dir)

        shp_files = list(local_dir.glob("*.shp"))
        if not shp_files:
            msg = f"No .shp file found in {s3_key}"
            raise ValueError(msg)
        if len(shp_files) > 1:
            logger.warning(f"Multiple .shp files found, using first: {shp_files[0]}")

        shp_path = shp_files[0]
        logger.info(f"Extracted shapefile: {shp_path}")
        return shp_path

    def _download_geojson(self, s3_key: str, local_dir: Path) -> Path:
        logger.info(f"Downloading GeoJSON from s3://{self.bucket_name}/{s3_key}")

        suffix = Path(s3_key).suffix.lower()
        if suffix not in (".json", ".geojson"):
            msg = (
                f"Unsupported GeoJSON extension: {suffix!r}. Expected .json or .geojson"
            )
            raise ValueError(msg)
        local_path = local_dir / f"input{suffix}"

        try:
            self.s3.download_file(self.bucket_name, s3_key, str(local_path))
        except ClientError as e:
            logger.error(f"Failed to download from S3: {e}")
            raise

        logger.info(f"Downloaded GeoJSON: {local_path}")
        return local_path
