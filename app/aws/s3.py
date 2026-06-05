"""Minimal S3 client for reference-data dump objects."""

import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class S3Client:
    """Reads dump objects under a fixed bucket/prefix."""

    def __init__(self, boto_client: Any, bucket: str, prefix: str = "") -> None:
        self._client = boto_client
        self._bucket = bucket
        self._prefix = prefix.strip("/")

    def _key(self, name: str) -> str:
        return f"{self._prefix}/{name}" if self._prefix else name

    def object_etag(self, name: str) -> str:
        resp = self._client.head_object(Bucket=self._bucket, Key=self._key(name))
        return resp["ETag"].strip('"')

    def download_object(self, name: str, dest: Path) -> None:
        logger.info("Downloading s3://%s/%s -> %s", self._bucket, self._key(name), dest)
        self._client.download_file(self._bucket, self._key(name), str(dest))
