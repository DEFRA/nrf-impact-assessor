"""Unit tests for job message models."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from app.models.job import (
    AssessmentType,
    BoundaryGeojson,
    ImpactAssessmentJob,
    IntersectingEdp,
)

# -- Fixtures --

SAMPLE_GEOJSON = {
    "boundaryGeometryOriginal": {
        "type": "Polygon",
        "coordinates": [
            [
                [582814.93, 328188.89],
                [582808.89, 328203.73],
                [582824.96, 328210.17],
                [582830.09, 328197.00],
                [582814.93, 328188.89],
            ]
        ],
    },
    "intersectingEdps": [
        {"label": "River Wensum SAC", "n2k_site_name": "River Wensum SAC"}
    ],
}


# -- Legacy format tests --


def test_legacy_job_valid():
    """Test creating a valid legacy ImpactAssessmentJob."""
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


def test_legacy_job_invalid_email():
    """Test that invalid email raises ValidationError."""
    with pytest.raises(ValidationError):
        ImpactAssessmentJob(
            job_id="test-123",
            developer_email="not-an-email",
        )


def test_legacy_job_default_submitted_at():
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


def test_legacy_job_json_serialization():
    """Test JSON serialization/deserialization for legacy format."""
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

    json_str = job.model_dump_json()
    job_restored = ImpactAssessmentJob.model_validate_json(json_str)

    assert job_restored.job_id == job.job_id
    assert job_restored.s3_input_key == job.s3_input_key
    assert job_restored.developer_email == job.developer_email
    assert job_restored.submitted_at == submitted
    assert job_restored.assessment_type == job.assessment_type
    assert job_restored.dwelling_type == job.dwelling_type
    assert job_restored.number_of_dwellings == job.number_of_dwellings


# -- Quote payload format tests --


def test_quote_payload_from_camel_case():
    """Test parsing a quote payload with camelCase keys (as sent by nrf-backend)."""
    payload = {
        "reference": "NRF-000001",
        "boundaryEntryType": "draw",
        "boundaryGeojson": SAMPLE_GEOJSON,
        "developmentTypes": ["housing"],
        "residentialBuildingCount": 25,
        "email": "dev@example.com",
        "wasteWaterTreatmentWorksId": "123",
        "wasteWaterTreatmentWorksName": "Some WwTW",
    }

    job = ImpactAssessmentJob.model_validate(payload)

    assert job.reference == "NRF-000001"
    assert job.boundary_entry_type == "draw"
    assert job.boundary_geojson is not None
    assert job.boundary_geojson.boundary_geometry_original["type"] == "Polygon"
    assert len(job.boundary_geojson.intersecting_edps) == 1
    assert job.boundary_geojson.intersecting_edps[0].label == "River Wensum SAC"
    assert job.development_types == ["housing"]
    assert job.residential_building_count == 25
    assert job.email == "dev@example.com"
    assert job.waste_water_treatment_works_id == "123"
    assert job.waste_water_treatment_works_name == "Some WwTW"


def test_quote_payload_invalid_reference():
    """Test that invalid reference format raises ValidationError."""
    with pytest.raises(ValidationError):
        ImpactAssessmentJob(reference="INVALID-REF")


def test_quote_payload_json_roundtrip():
    """Test JSON serialization/deserialization for quote format."""
    payload = {
        "reference": "NRF-000001",
        "boundaryGeojson": SAMPLE_GEOJSON,
        "developmentTypes": ["housing"],
        "residentialBuildingCount": 25,
        "email": "dev@example.com",
    }

    job = ImpactAssessmentJob.model_validate(payload)
    json_str = job.model_dump_json(by_alias=True)
    job_restored = ImpactAssessmentJob.model_validate_json(json_str)

    assert job_restored.reference == "NRF-000001"
    assert job_restored.boundary_geojson is not None
    assert job_restored.residential_building_count == 25


# -- Effective property tests --


def test_effective_id_prefers_job_id():
    """Test effective_id returns job_id when both are present."""
    job = ImpactAssessmentJob(job_id="test-123", reference="NRF-000001")
    assert job.effective_id == "test-123"


def test_effective_id_falls_back_to_reference():
    """Test effective_id returns reference when job_id is absent."""
    job = ImpactAssessmentJob(reference="NRF-000001")
    assert job.effective_id == "NRF-000001"


def test_effective_id_unknown_fallback():
    """Test effective_id returns 'unknown' when both are absent."""
    job = ImpactAssessmentJob()
    assert job.effective_id == "unknown"


def test_effective_dwelling_type_from_legacy():
    """Test effective_dwelling_type prefers legacy dwelling_type."""
    job = ImpactAssessmentJob(dwelling_type="apartment", development_types=["housing"])
    assert job.effective_dwelling_type == "apartment"


def test_effective_dwelling_type_from_quote():
    """Test effective_dwelling_type falls back to development_types."""
    job = ImpactAssessmentJob(development_types=["other-residential"])
    assert job.effective_dwelling_type == "other-residential"


def test_effective_dwellings_from_legacy():
    """Test effective_dwellings prefers legacy number_of_dwellings."""
    job = ImpactAssessmentJob(number_of_dwellings=10, residential_building_count=25)
    assert job.effective_dwellings == 10


def test_effective_dwellings_from_quote():
    """Test effective_dwellings falls back to residential_building_count."""
    job = ImpactAssessmentJob(residential_building_count=25)
    assert job.effective_dwellings == 25


# -- BoundaryGeojson tests --


def test_boundary_geojson_from_alias():
    """Test BoundaryGeojson parses camelCase aliases."""
    bg = BoundaryGeojson.model_validate(SAMPLE_GEOJSON)

    assert bg.boundary_geometry_original["type"] == "Polygon"
    assert len(bg.intersecting_edps) == 1
    assert bg.intersecting_edps[0].n2k_site_name == "River Wensum SAC"


def test_intersecting_edp():
    """Test IntersectingEdp model."""
    edp = IntersectingEdp(label="Test EDP", n2k_site_name="Test Site")
    assert edp.label == "Test EDP"
    assert edp.n2k_site_name == "Test Site"
