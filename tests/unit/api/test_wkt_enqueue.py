"""Unit tests for POST /test/enqueue (SQS enqueue endpoint)."""

import json
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.test.router import router

_app = FastAPI()
_app.include_router(router, prefix="/test")
client = TestClient(_app)

_POLYGON_BNG = "POLYGON ((450000 100000, 450500 100000, 450500 100500, 450000 100500, 450000 100000))"

_VALID_BODY = {
    "wkt": _POLYGON_BNG,
    "assessment_type": "nutrient",
    "dwelling_type": "house",
    "dwellings": 10,
    "name": "Test Development",
    "developer_email": "test@example.com",
}


def _make_aws_config(
    bucket="nrf-inputs", queue="http://localhost:4566/000000000000/nrf-queue"
):
    mock = MagicMock()
    mock.s3_input_bucket = bucket
    mock.sqs_queue_url = queue
    mock.region = "eu-west-2"
    mock.endpoint_url = "http://localhost:4566"
    return mock


@pytest.fixture(autouse=True)
def mock_aws_config():
    with patch("app.test.router.AWSConfig", return_value=_make_aws_config()):
        yield


@pytest.fixture(autouse=True)
def mock_boto3():
    """Mock both S3 and SQS boto3 clients."""
    mock_s3 = MagicMock()
    mock_sqs = MagicMock()
    mock_sqs.send_message.return_value = {"MessageId": "mock-message-id-123"}

    def boto3_client_factory(service, **kwargs):
        if service == "s3":
            return mock_s3
        if service == "sqs":
            return mock_sqs
        return MagicMock()

    with patch(
        "app.test.router.boto3.client", side_effect=boto3_client_factory
    ) as mock:
        yield {"client": mock, "s3": mock_s3, "sqs": mock_sqs}


class TestWktEnqueueEndpoint:
    def test_valid_request_returns_202(self):
        response = client.post("/test/enqueue", json=_VALID_BODY)
        assert response.status_code == 202

    def test_response_schema(self):
        response = client.post("/test/enqueue", json=_VALID_BODY)
        body = response.json()
        assert "job_id" in body
        assert "s3_key" in body
        assert "message_id" in body
        assert "note" in body
        assert body["message_id"] == "mock-message-id-123"

    def test_s3_key_format(self):
        response = client.post("/test/enqueue", json=_VALID_BODY)
        s3_key = response.json()["s3_key"]
        job_id = response.json()["job_id"]
        assert s3_key == f"jobs/{job_id}/input.geojson"

    def test_s3_put_object_called(self, mock_boto3):
        client.post("/test/enqueue", json=_VALID_BODY)
        mock_boto3["s3"].put_object.assert_called_once()
        call_kwargs = mock_boto3["s3"].put_object.call_args.kwargs
        assert call_kwargs["Bucket"] == "nrf-inputs"
        assert call_kwargs["Key"].endswith("/input.geojson")
        assert isinstance(call_kwargs["Body"], bytes)

    def test_geojson_bytes_are_valid(self, mock_boto3):
        client.post("/test/enqueue", json=_VALID_BODY)
        body_bytes = mock_boto3["s3"].put_object.call_args.kwargs["Body"]
        geojson = json.loads(body_bytes)
        assert geojson["type"] == "FeatureCollection"
        assert len(geojson["features"]) == 1

    def test_sqs_message_body_is_valid_job(self, mock_boto3):
        client.post("/test/enqueue", json=_VALID_BODY)
        mock_boto3["sqs"].send_message.assert_called_once()
        call_kwargs = mock_boto3["sqs"].send_message.call_args.kwargs
        assert call_kwargs["QueueUrl"] == "http://localhost:4566/000000000000/nrf-queue"
        message = json.loads(call_kwargs["MessageBody"])
        assert message["assessment_type"] == "nutrient"
        assert message["dwelling_type"] == "house"
        assert message["number_of_dwellings"] == 10
        assert message["developer_email"] == "test@example.com"

    def test_job_id_consistent_across_s3_and_sqs(self, mock_boto3):
        response = client.post("/test/enqueue", json=_VALID_BODY)
        job_id = response.json()["job_id"]

        s3_key = mock_boto3["s3"].put_object.call_args.kwargs["Key"]
        message = json.loads(
            mock_boto3["sqs"].send_message.call_args.kwargs["MessageBody"]
        )

        assert s3_key == f"jobs/{job_id}/input.geojson"
        assert message["job_id"] == job_id
        assert message["s3_input_key"] == f"jobs/{job_id}/input.geojson"

    def test_note_contains_job_id(self):
        response = client.post("/test/enqueue", json=_VALID_BODY)
        body = response.json()
        assert body["job_id"] in body["note"]

    def test_invalid_assessment_type_returns_400(self):
        response = client.post(
            "/test/enqueue", json={**_VALID_BODY, "assessment_type": "bad"}
        )
        assert response.status_code == 400
        assert "assessment_type" in response.json()["detail"]

    def test_invalid_wkt_returns_400(self):
        response = client.post(
            "/test/enqueue", json={**_VALID_BODY, "wkt": "NOT VALID WKT"}
        )
        assert response.status_code == 400
        assert "Invalid WKT" in response.json()["detail"]

    def test_missing_bucket_returns_400(self):
        with patch(
            "app.test.router.AWSConfig", return_value=_make_aws_config(bucket="")
        ):
            response = client.post("/test/enqueue", json=_VALID_BODY)
        assert response.status_code == 400
        assert "AWS_S3_INPUT_BUCKET" in response.json()["detail"]

    def test_missing_queue_url_returns_400(self):
        with patch(
            "app.test.router.AWSConfig", return_value=_make_aws_config(queue="")
        ):
            response = client.post("/test/enqueue", json=_VALID_BODY)
        assert response.status_code == 400
        assert "AWS_SQS_QUEUE_URL" in response.json()["detail"]

    def test_s3_failure_returns_502(self, mock_boto3):
        from botocore.exceptions import ClientError

        mock_boto3["s3"].put_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchBucket", "Message": "bucket not found"}},
            "PutObject",
        )
        response = client.post("/test/enqueue", json=_VALID_BODY)
        assert response.status_code == 502
        assert "S3 upload failed" in response.json()["detail"]

    def test_sqs_failure_returns_502(self, mock_boto3):
        from botocore.exceptions import ClientError

        mock_boto3["sqs"].send_message.side_effect = ClientError(
            {"Error": {"Code": "QueueDoesNotExist", "Message": "queue not found"}},
            "SendMessage",
        )
        response = client.post("/test/enqueue", json=_VALID_BODY)
        assert response.status_code == 502
        assert "SQS send failed" in response.json()["detail"]

    def test_wgs84_wkt_is_reprojected(self, mock_boto3):
        wkt_wgs84 = "POLYGON ((-1.5 52.5, -1.4 52.5, -1.4 52.6, -1.5 52.6, -1.5 52.5))"
        response = client.post(
            "/test/enqueue",
            json={**_VALID_BODY, "wkt": wkt_wgs84, "crs": "EPSG:4326"},
        )
        assert response.status_code == 202
        geojson = json.loads(mock_boto3["s3"].put_object.call_args.kwargs["Body"])
        # GeoDataFrame.to_json() always outputs WGS84 for GeoJSON spec compliance
        assert geojson["type"] == "FeatureCollection"

    def test_default_developer_email(self, mock_boto3):
        body = {k: v for k, v in _VALID_BODY.items() if k != "developer_email"}
        client.post("/test/enqueue", json=body)
        message = json.loads(
            mock_boto3["sqs"].send_message.call_args.kwargs["MessageBody"]
        )
        assert message["developer_email"] == "test@example.com"

    def test_boto3_uses_localstack_endpoint(self, mock_boto3):
        client.post("/test/enqueue", json=_VALID_BODY)
        calls = mock_boto3["client"].call_args_list
        for call in calls:
            assert call.kwargs.get("endpoint_url") == "http://localhost:4566"
