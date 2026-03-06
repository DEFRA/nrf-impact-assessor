"""Unit tests for job message models."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from app.models.job import AssessmentType, ImpactAssessmentJob


def test_impact_assessment_job_valid():
    """Test creating a valid ImpactAssessmentJob."""
    submitted = datetime(2025, 10, 15, 14, 30, 0, tzinfo=UTC)
    job = ImpactAssessmentJob(
        job_id="test-123",
        s3_input_key="jobs/test-123/input.zip",
        developer_email="developer@example.com",
        submitted_at=submitted,
        assessment_type="nutrient",
        dwelling_type="house",
        number_of_dwellings=5,
    )

    assert job.job_id == "test-123"
    assert job.s3_input_key == "jobs/test-123/input.zip"
    assert job.developer_email == "developer@example.com"
    assert job.submitted_at == submitted
    assert job.assessment_type == AssessmentType.NUTRIENT
    assert job.dwelling_type == "house"
    assert job.number_of_dwellings == 5


def test_impact_assessment_job_invalid_email():
    """Test that invalid email raises ValidationError."""
    with pytest.raises(ValidationError):
        ImpactAssessmentJob(
            job_id="test-123",
            s3_input_key="jobs/test-123/input.zip",
            developer_email="not-an-email",  # Invalid email format
        )


def test_impact_assessment_job_default_submitted_at():
    """Test that submitted_at defaults to current time."""
    before = datetime.now(UTC)
    job = ImpactAssessmentJob(
        job_id="test-123",
        s3_input_key="jobs/test-123/input.zip",
        developer_email="developer@example.com",
        assessment_type="nutrient",
        dwelling_type="apartment",
        number_of_dwellings=20,
    )
    after = datetime.now(UTC)

    assert before <= job.submitted_at <= after


def test_impact_assessment_job_json_serialization():
    """Test JSON serialization/deserialization."""
    submitted = datetime(2025, 10, 15, 14, 30, 0, tzinfo=UTC)
    job = ImpactAssessmentJob(
        job_id="test-123",
        s3_input_key="jobs/test-123/input.zip",
        developer_email="developer@example.com",
        submitted_at=submitted,
        assessment_type="nutrient",
        dwelling_type="house",
        number_of_dwellings=3,
    )

    # Serialize to JSON
    json_str = job.model_dump_json()

    # Deserialize back
    job_restored = ImpactAssessmentJob.model_validate_json(json_str)

    assert job_restored.job_id == job.job_id
    assert job_restored.s3_input_key == job.s3_input_key
    assert job_restored.developer_email == job.developer_email
    assert job_restored.submitted_at == submitted
    assert job_restored.assessment_type == job.assessment_type
    assert job_restored.dwelling_type == job.dwelling_type
    assert job_restored.number_of_dwellings == job.number_of_dwellings
