"""Unit tests for job message models."""

import pytest
from pydantic import ValidationError

from app.models.job import BoundaryGeojson, ImpactAssessmentJob, IntersectingEdp

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
    "intersectingEdps": [{"label": "Broads SAC (Yare & Bure) & Wensum SAC"}],
}


# -- Quote payload tests --


def test_quote_payload_from_camel_case():
    """Parse a quote payload with camelCase keys as sent by nrf-backend."""
    payload = {
        "reference": "NRF-000001",
        "boundaryGeojson": SAMPLE_GEOJSON,
        "developmentTypes": ["housing"],
        "housingUnits": 25,
        "wasteWaterTreatmentWorksId": "123",
    }

    job = ImpactAssessmentJob.model_validate(payload)

    assert job.reference == "NRF-000001"
    assert job.boundary_geojson is not None
    assert job.boundary_geojson.boundary_geometry_original["type"] == "Polygon"
    assert len(job.boundary_geojson.intersecting_edps) == 1
    assert (
        job.boundary_geojson.intersecting_edps[0].label
        == "Broads SAC (Yare & Bure) & Wensum SAC"
    )
    assert job.development_types == ["housing"]
    assert job.residential_building_count == 25
    assert job.waste_water_treatment_works_id == "123"


def test_quote_payload_invalid_reference():
    """Invalid reference format raises ValidationError."""
    with pytest.raises(ValidationError):
        ImpactAssessmentJob(reference="INVALID-REF")


def test_quote_payload_json_roundtrip():
    """JSON serialization/deserialization preserves quote fields."""
    payload = {
        "reference": "NRF-000001",
        "boundaryGeojson": SAMPLE_GEOJSON,
        "developmentTypes": ["housing"],
        "housingUnits": 25,
    }

    job = ImpactAssessmentJob.model_validate(payload)
    json_str = job.model_dump_json(by_alias=True)
    job_restored = ImpactAssessmentJob.model_validate_json(json_str)

    assert job_restored.reference == "NRF-000001"
    assert job_restored.boundary_geojson is not None
    assert job_restored.residential_building_count == 25


# -- BoundaryGeojson tests --


def test_boundary_geojson_from_alias():
    """BoundaryGeojson parses camelCase aliases."""
    bg = BoundaryGeojson.model_validate(SAMPLE_GEOJSON)

    assert bg.boundary_geometry_original["type"] == "Polygon"
    assert len(bg.intersecting_edps) == 1
    assert bg.intersecting_edps[0].label == "Broads SAC (Yare & Bure) & Wensum SAC"


def test_intersecting_edp():
    """IntersectingEdp model holds the EDP label."""
    edp = IntersectingEdp(label="Test EDP")
    assert edp.label == "Test EDP"


def test_intersecting_edp_ignores_retired_fields():
    """Messages queued before the payload was trimmed must still parse.

    `/check-boundary` used to emit `n2k_site_name` and the EDP/intersection
    polygons; anything still in flight on the queue carries them.
    """
    edp = IntersectingEdp.model_validate(
        {
            "label": "Test EDP",
            "n2k_site_name": "Test Site",
            "edp_geometry": {"type": "Polygon", "coordinates": []},
            "intersection_geometry": {"type": "Polygon", "coordinates": []},
        }
    )

    assert edp.label == "Test EDP"
    assert not hasattr(edp, "n2k_site_name")
