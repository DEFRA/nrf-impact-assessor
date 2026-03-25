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
        key_prefix = "/".join(s3_key.split("/")[:2])
        logger.info(
            f"Downloading shapefile ZIP from s3://{self.bucket_name}/{key_prefix}/..."
        )

        zip_path = local_dir / "input.zip"
        try:
            self.s3.download_file(self.bucket_name, s3_key, str(zip_path))
        except ClientError as e:
            logger.error(
                f"Failed to download shapefile ZIP from S3: {e.response['Error']['Code']}"
            )
            raise

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            # Validate every member before extraction (Zip Slip defence).
            resolved_local = local_dir.resolve()
            for member in zip_ref.infolist():
                member_path = (local_dir / member.filename).resolve()
                if not member_path.is_relative_to(resolved_local):
                    msg = f"Invalid zip: entry '{member.filename}' would extract outside target directory"
                    raise ValueError(msg)
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
        key_prefix = "/".join(s3_key.split("/")[:2])
        logger.info(
            f"Downloading GeoJSON from s3://{self.bucket_name}/{key_prefix}/..."
        )

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
            logger.error(
                f"Failed to download GeoJSON from S3: {e.response['Error']['Code']}"
            )
            raise

        logger.info(f"Downloaded GeoJSON: {local_path}")
        return local_path
