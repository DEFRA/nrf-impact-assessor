import json
from unittest.mock import MagicMock

from app.aws.s3 import S3Client


def _mock_boto_with_manifest(payload: dict) -> MagicMock:
    body = MagicMock()
    body.read.return_value = json.dumps(payload).encode()
    client = MagicMock()
    client.get_object.return_value = {"Body": body}
    client.head_object.return_value = {"ETag": '"abc123"'}
    return client


def test_read_manifest_parses_json():
    boto = _mock_boto_with_manifest(
        {"data_version": "v1", "tables": {"nn_catchments": "x.sql.gz"}}
    )
    s3 = S3Client(boto, bucket="b", prefix="p")
    manifest = s3.read_manifest("manifest.json")
    assert manifest.data_version == "v1"
    boto.get_object.assert_called_once_with(Bucket="b", Key="p/manifest.json")


def test_object_etag_strips_quotes():
    boto = _mock_boto_with_manifest({"data_version": "v1", "tables": {"a": "b"}})
    s3 = S3Client(boto, bucket="b", prefix="p")
    assert s3.object_etag("nn_catchments.sql.gz") == "abc123"
    boto.head_object.assert_called_once_with(Bucket="b", Key="p/nn_catchments.sql.gz")


def test_download_object_writes_file(tmp_path):
    boto = _mock_boto_with_manifest({"data_version": "v1", "tables": {"a": "b"}})
    s3 = S3Client(boto, bucket="b", prefix="p")
    dest = tmp_path / "out.gz"
    s3.download_object("nn.sql.gz", dest)
    boto.download_file.assert_called_once_with("b", "p/nn.sql.gz", str(dest))


def test_key_without_prefix_uses_bare_key():
    boto = _mock_boto_with_manifest(
        {"data_version": "v1", "tables": {"nn_catchments": "x.sql.gz"}}
    )
    s3 = S3Client(boto, bucket="b", prefix="")
    s3.read_manifest("manifest.json")
    boto.get_object.assert_called_once_with(Bucket="b", Key="manifest.json")


def test_prefix_slashes_are_normalised():
    boto = _mock_boto_with_manifest(
        {"data_version": "v1", "tables": {"nn_catchments": "x.sql.gz"}}
    )
    s3 = S3Client(boto, bucket="b", prefix="/dumps/")
    s3.read_manifest("manifest.json")
    boto.get_object.assert_called_once_with(Bucket="b", Key="dumps/manifest.json")
