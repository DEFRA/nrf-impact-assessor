"""Minimal S3 client for reference-data dump objects."""

import logging
from pathlib import Path
from typing import Any

from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

_NOT_FOUND_CODES = {"404", "NoSuchKey", "NoSuchBucket"}


class S3ObjectError(RuntimeError):
    """An S3 object could not be accessed; message carries bucket/key context."""


class S3Client:
    """Reads dump objects under a fixed bucket/prefix."""

    def __init__(self, boto_client: Any, bucket: str, prefix: str = "") -> None:
        if not bucket:
            msg = "DATA_SYNC_S3_BUCKET is not configured"
            raise S3ObjectError(msg)
        self._client = boto_client
        self._bucket = bucket
        self._prefix = prefix.strip("/")

    def _key(self, name: str) -> str:
        return f"{self._prefix}/{name}" if self._prefix else name

    def _wrap(self, key: str, exc: ClientError) -> S3ObjectError:
        """Turn an opaque botocore ClientError into an actionable message."""
        code = exc.response.get("Error", {}).get("Code", "")
        location = f"s3://{self._bucket}/{key}"
        if code in _NOT_FOUND_CODES:
            return S3ObjectError(
                f"reference data dump not found: {location} "
                "(check DATA_SYNC_S3_BUCKET/DATA_SYNC_S3_PREFIX and the manifest key; "
                "do not repeat the prefix in the key)"
            )
        return S3ObjectError(f"S3 {code or 'error'} accessing {location}")

    def object_etag(self, name: str) -> str:
        key = self._key(name)
        try:
            resp = self._client.head_object(Bucket=self._bucket, Key=key)
        except ClientError as exc:
            raise self._wrap(key, exc) from exc
        return resp["ETag"].strip('"')

    def download_object(self, name: str, dest: Path) -> None:
        key = self._key(name)
        logger.info("Downloading s3://%s/%s -> %s", self._bucket, key, dest)
        try:
            self._client.download_file(self._bucket, key, str(dest))
        except ClientError as exc:
            raise self._wrap(key, exc) from exc
