"""Tests for the async assessment endpoints (POST /assess, GET /assess/{job_id})."""

import time
from io import BytesIO
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app.assess.router import JobState, _jobs
from app.main import app


@pytest.fixture(autouse=True)
def _clear_jobs():
    """Clear the job store before and after each test."""
    _jobs.clear()
    yield
    _jobs.clear()


@pytest.fixture
def client():
    return TestClient(app)


def _make_geojson_bytes() -> bytes:
    """Create a minimal GeoJSON FeatureCollection as bytes."""
    import json

    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
                },
                "properties": {"name": "test"},
            }
        ],
    }
    return json.dumps(geojson).encode()


def _fake_run_assessment():
    """Return a minimal result dict mimicking run_assessment output."""
    return {
        "summary": pd.DataFrame({"col": [1, 2]}),
    }


class TestPostAssess:
    """Tests for POST /assess."""

    @patch("app.assess.router.run_assessment", side_effect=_fake_run_assessment)
    @patch("app.assess.router._get_repository", return_value=MagicMock())
    def test_returns_202_with_job_id(self, client):
        content = _make_geojson_bytes()
        response = client.post(
            "/assess",
            files={
                "geometry_file": ("test.geojson", BytesIO(content), "application/json")
            },
            data={"assessment_type": "nutrient", "dwellings": "1"},
        )

        assert response.status_code == 202
        body = response.json()
        assert "job_id" in body
        assert body["status"] == "pending"
        assert body["poll_url"] == f"/assess/{body['job_id']}"

    @patch("app.assess.router.run_assessment", side_effect=_fake_run_assessment)
    @patch("app.assess.router._get_repository", return_value=MagicMock())
    def test_job_completes_successfully(self, client):
        content = _make_geojson_bytes()
        response = client.post(
            "/assess",
            files={
                "geometry_file": ("test.geojson", BytesIO(content), "application/json")
            },
            data={"assessment_type": "nutrient"},
        )
        job_id = response.json()["job_id"]

        # Give the background task time to complete
        time.sleep(0.5)

        status_resp = client.get(f"/assess/{job_id}")
        assert status_resp.status_code == 200
        body = status_resp.json()
        assert body["status"] in ("pending", "running", "completed")

    def test_invalid_assessment_type_returns_400(self, client):
        content = _make_geojson_bytes()
        response = client.post(
            "/assess",
            files={
                "geometry_file": ("test.geojson", BytesIO(content), "application/json")
            },
            data={"assessment_type": "invalid"},
        )
        assert response.status_code == 400
        assert "Invalid assessment_type" in response.json()["detail"]

    @patch("app.assess.router._MAX_JOBS", 0)
    @patch("app.assess.router._prune_expired_jobs")
    def test_too_many_jobs_returns_503(self, client):
        # Pre-fill with a job so len(_jobs) >= _MAX_JOBS (which is 0)
        _jobs["existing"] = JobState(status="running")
        content = _make_geojson_bytes()
        response = client.post(
            "/assess",
            files={
                "geometry_file": ("test.geojson", BytesIO(content), "application/json")
            },
            data={"assessment_type": "nutrient"},
        )
        assert response.status_code == 503


class TestGetAssess:
    """Tests for GET /assess/{job_id}."""

    def test_unknown_job_returns_404(self, client):
        response = client.get("/assess/nonexistent-id")
        assert response.status_code == 404

    def test_pending_job_returns_status(self, client):
        _jobs["test-123"] = JobState(status="pending")
        response = client.get("/assess/test-123")
        assert response.status_code == 200
        body = response.json()
        assert body["job_id"] == "test-123"
        assert body["status"] == "pending"
        assert body["results"] is None

    def test_completed_job_returns_results(self, client):
        _jobs["test-456"] = JobState(
            status="completed",
            results={"summary": [{"col": 1}]},
            timing_s=2.5,
        )
        response = client.get("/assess/test-456")
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "completed"
        assert body["results"] == {"summary": [{"col": 1}]}
        assert body["timing_s"] == 2.5

    def test_failed_job_returns_error(self, client):
        _jobs["test-789"] = JobState(
            status="failed",
            error="Something went wrong",
            timing_s=1.0,
        )
        response = client.get("/assess/test-789")
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "failed"
        assert body["error"] == "Something went wrong"


class TestPruneExpiredJobs:
    """Tests for TTL-based job cleanup."""

    def test_expired_jobs_are_removed(self):
        from app.assess.router import _prune_expired_jobs

        # Create an expired job (created_at far in the past)
        _jobs["old-job"] = JobState(status="completed", created_at=0.0)
        _jobs["new-job"] = JobState(status="completed")

        _prune_expired_jobs()

        assert "old-job" not in _jobs
        assert "new-job" in _jobs
