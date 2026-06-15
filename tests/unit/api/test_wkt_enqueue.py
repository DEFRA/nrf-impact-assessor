"""Unit tests for POST /test/enqueue (SQS enqueue endpoint)."""

import json
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.common.tracing import TraceIdMiddleware, ctx_trace_id
from app.test.router import router

_app = FastAPI()
# Mount the same tracing middleware the production app uses, so the
# `x-cdp-request-id` header drives ctx_trace_id during the request and the
# /test/enqueue handler can inherit it.
_app.add_middleware(TraceIdMiddleware)
_app.include_router(router, prefix="/test")
client = TestClient(_app)

_POLYGON_BNG = "POLYGON ((450000 100000, 450500 100000, 450500 100500, 450000 100500, 450000 100000))"

_VALID_BODY = {
    "wkt": _POLYGON_BNG,
    "assessment_type": "nutrient",
    "dwelling_type": "house",
    "dwellings": 10,
    "name": "Test Development",
}


def _make_aws_config(queue="http://localhost:4566/000000000000/nrf-queue"):
    mock = MagicMock()
    mock.sqs_queue_url = queue
    mock.region = "eu-west-2"
    mock.endpoint_url = "http://localhost:4566"
    return mock


@pytest.fixture(autouse=True)
def isolate_ctx_trace_id():
    """Reset ctx_trace_id around each test.

    Other modules (e.g. tests/common/test_http_client.py) set ctx_trace_id
    without resetting, which would otherwise leak into the "neither supplied"
    case here and make it look like a header was sent.
    """
    token = ctx_trace_id.set("")
    yield
    ctx_trace_id.reset(token)


@pytest.fixture(autouse=True)
def mock_aws_config():
    with patch("app.test.router.AWSConfig", return_value=_make_aws_config()):
        yield


@pytest.fixture(autouse=True)
def mock_boto3():
    """Mock SQS boto3 client."""
    mock_sqs = MagicMock()
    mock_sqs.send_message.return_value = {"MessageId": "mock-message-id-123"}

    def boto3_client_factory(service, **kwargs):
        if service == "sqs":
            return mock_sqs
        return MagicMock()

    with patch(
        "app.test.router.boto3.client", side_effect=boto3_client_factory
    ) as mock:
        yield {"client": mock, "sqs": mock_sqs}


class TestWktEnqueueEndpoint:
    def test_valid_request_returns_202(self):
        response = client.post("/test/enqueue", json=_VALID_BODY)
        assert response.status_code == 202

    def test_response_schema(self):
        response = client.post("/test/enqueue", json=_VALID_BODY)
        body = response.json()
        assert "job_id" in body
        assert "message_id" in body
        assert "note" in body
        assert body["message_id"] == "mock-message-id-123"

    def test_sqs_message_contains_geometry(self, mock_boto3):
        client.post("/test/enqueue", json=_VALID_BODY)
        mock_boto3["sqs"].send_message.assert_called_once()
        call_kwargs = mock_boto3["sqs"].send_message.call_args.kwargs
        message = json.loads(call_kwargs["MessageBody"])
        geom = message["boundaryGeojson"]["boundaryGeometryOriginal"]
        assert geom["type"] == "Polygon"
        assert len(geom["coordinates"][0]) >= 4

    def test_sqs_message_body_is_valid_job(self, mock_boto3):
        client.post("/test/enqueue", json=_VALID_BODY)
        mock_boto3["sqs"].send_message.assert_called_once()
        call_kwargs = mock_boto3["sqs"].send_message.call_args.kwargs
        assert call_kwargs["QueueUrl"] == "http://localhost:4566/000000000000/nrf-queue"
        message = json.loads(call_kwargs["MessageBody"])
        assert message["reference"].startswith("NRF-")
        assert len(message["reference"]) == 10  # NRF-######
        assert message["developmentTypes"] == ["house"]
        assert message["residentialBuildingCount"] == 10

    def test_job_id_matches_reference_in_message(self, mock_boto3):
        response = client.post("/test/enqueue", json=_VALID_BODY)
        job_id = response.json()["job_id"]
        message = json.loads(
            mock_boto3["sqs"].send_message.call_args.kwargs["MessageBody"]
        )
        assert message["reference"] == job_id

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

    def test_missing_queue_url_returns_400(self):
        with patch(
            "app.test.router.AWSConfig", return_value=_make_aws_config(queue="")
        ):
            response = client.post("/test/enqueue", json=_VALID_BODY)
        assert response.status_code == 400
        assert "AWS_SQS_QUEUE_URL" in response.json()["detail"]

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
        message = json.loads(
            mock_boto3["sqs"].send_message.call_args.kwargs["MessageBody"]
        )
        geom = message["boundaryGeojson"]["boundaryGeometryOriginal"]
        assert geom["type"] == "Polygon"

    def test_boto3_uses_localstack_endpoint(self, mock_boto3):
        client.post("/test/enqueue", json=_VALID_BODY)
        calls = mock_boto3["client"].call_args_list
        for call in calls:
            assert call.kwargs.get("endpoint_url") == "http://localhost:4566"

    def test_trace_id_in_body_lands_in_queued_message(self, mock_boto3):
        client.post(
            "/test/enqueue", json={**_VALID_BODY, "trace_id": "trace-from-body"}
        )
        message = json.loads(
            mock_boto3["sqs"].send_message.call_args.kwargs["MessageBody"]
        )
        assert message["traceId"] == "trace-from-body"

    def test_trace_id_inherited_from_request_header(self, mock_boto3):
        client.post(
            "/test/enqueue",
            json=_VALID_BODY,
            headers={"x-cdp-request-id": "trace-from-header"},
        )
        message = json.loads(
            mock_boto3["sqs"].send_message.call_args.kwargs["MessageBody"]
        )
        assert message["traceId"] == "trace-from-header"

    def test_body_trace_id_overrides_request_header(self, mock_boto3):
        client.post(
            "/test/enqueue",
            json={**_VALID_BODY, "trace_id": "wins"},
            headers={"x-cdp-request-id": "loses"},
        )
        message = json.loads(
            mock_boto3["sqs"].send_message.call_args.kwargs["MessageBody"]
        )
        assert message["traceId"] == "wins"

    def test_no_trace_id_when_neither_supplied(self, mock_boto3):
        client.post("/test/enqueue", json=_VALID_BODY)
        message = json.loads(
            mock_boto3["sqs"].send_message.call_args.kwargs["MessageBody"]
        )
        assert message.get("traceId") is None
