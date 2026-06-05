from unittest.mock import MagicMock

from app.aws.s3 import S3Client


def _mock_boto() -> MagicMock:
    client = MagicMock()
    client.head_object.return_value = {"ETag": '"abc123"'}
    return client


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
