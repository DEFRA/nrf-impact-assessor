from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from app.aws.s3 import S3Client, S3ObjectError


def _mock_boto() -> MagicMock:
    client = MagicMock()
    client.head_object.return_value = {"ETag": '"abc123"'}
    return client


def _client_error(code: str, op: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, op)


def test_object_etag_strips_quotes():
    boto = _mock_boto()
    s3 = S3Client(boto, bucket="b", prefix="p")
    assert s3.object_etag("nn_catchments.sql.gz") == "abc123"
    boto.head_object.assert_called_once_with(Bucket="b", Key="p/nn_catchments.sql.gz")


def test_download_object_writes_file(tmp_path):
    boto = _mock_boto()
    s3 = S3Client(boto, bucket="b", prefix="p")
    dest = tmp_path / "out.gz"
    s3.download_object("nn.sql.gz", dest)
    boto.download_file.assert_called_once_with("b", "p/nn.sql.gz", str(dest))


def test_key_without_prefix_uses_bare_key():
    boto = _mock_boto()
    s3 = S3Client(boto, bucket="b", prefix="")
    s3.object_etag("nn.sql.gz")
    boto.head_object.assert_called_once_with(Bucket="b", Key="nn.sql.gz")


def test_prefix_slashes_are_normalised():
    boto = _mock_boto()
    s3 = S3Client(boto, bucket="b", prefix="/dumps/")
    s3.object_etag("nn.sql.gz")
    boto.head_object.assert_called_once_with(Bucket="b", Key="dumps/nn.sql.gz")


def test_empty_bucket_raises_clear_error():
    with pytest.raises(S3ObjectError, match="DATA_SYNC_S3_BUCKET is not configured"):
        S3Client(MagicMock(), bucket="", prefix="p")


def test_object_etag_not_found_names_bucket_and_key():
    boto = MagicMock()
    boto.head_object.side_effect = _client_error("404", "HeadObject")
    s3 = S3Client(boto, bucket="b", prefix="dumps")
    with pytest.raises(S3ObjectError, match=r"not found: s3://b/dumps/nn.sql.gz"):
        s3.object_etag("nn.sql.gz")


def test_download_object_not_found_names_bucket_and_key(tmp_path):
    boto = MagicMock()
    boto.download_file.side_effect = _client_error("NoSuchKey", "GetObject")
    s3 = S3Client(boto, bucket="b", prefix="")
    with pytest.raises(S3ObjectError, match=r"not found: s3://b/nn.sql.gz"):
        s3.download_object("nn.sql.gz", tmp_path / "out.gz")


def test_other_s3_errors_keep_context():
    boto = MagicMock()
    boto.head_object.side_effect = _client_error("403", "HeadObject")
    s3 = S3Client(boto, bucket="b", prefix="dumps")
    with pytest.raises(S3ObjectError, match=r"S3 403 accessing s3://b/dumps/nn.sql.gz"):
        s3.object_etag("nn.sql.gz")
