"""Minimal S3 client for reference-data dumps and the manifest."""

import json
import logging
from pathlib import Path
from typing import Any

from app.data_sync.manifest import Manifest, parse_manifest

logger = logging.getLogger(__name__)


class S3Client:
    """Reads the manifest and dump objects under a fixed bucket/prefix."""

    def __init__(self, boto_client: Any, bucket: str, prefix: str = "") -> None:
        self._client = boto_client
        self._bucket = bucket
        self._prefix = prefix.strip("/")

    def _key(self, name: str) -> str:
        return f"{self._prefix}/{name}" if self._prefix else name

    def read_manifest(self, manifest_key: str) -> Manifest:
        resp = self._client.get_object(Bucket=self._bucket, Key=self._key(manifest_key))
        raw = json.loads(resp["Body"].read())
        return parse_manifest(raw)

    def object_etag(self, name: str) -> str:
        resp = self._client.head_object(Bucket=self._bucket, Key=self._key(name))
        return resp["ETag"].strip('"')

    def download_object(self, name: str, dest: Path) -> None:
        logger.info("Downloading s3://%s/%s -> %s", self._bucket, self._key(name), dest)
        self._client.download_file(self._bucket, self._key(name), str(dest))
