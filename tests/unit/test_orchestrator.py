"""process_job must signal failure (raise) rather than swallow it into an
empty dict, so the consumer can tell a completed assessment from a failed one
and leave failed messages on the queue."""

from unittest.mock import MagicMock

import geopandas as gpd
import pytest
from shapely.geometry import Polygon

from app.assessments.reference_data import EmptyReferenceDataError
from app.models.enums import AssessmentType
from app.orchestrator import JobOrchestrator, JobProcessingError


def _job(reference: str = "NRF-000001"):
    job = MagicMock()
    job.reference = reference
    job.trace_id = None
    job.boundary_geojson = MagicMock()
    return job


def _orchestrator() -> JobOrchestrator:
    return JobOrchestrator(MagicMock(), MagicMock(), MagicMock())


def test_propagates_empty_reference_data(mocker):
    mocker.patch(
        "app.orchestrator.assert_reference_data_present",
        side_effect=EmptyReferenceDataError("wwtw_catchments is empty"),
    )
    with pytest.raises(EmptyReferenceDataError):
        _orchestrator().process_job(_job(), AssessmentType.NUTRIENT)


def test_raises_when_geometry_missing(mocker):
    mocker.patch("app.orchestrator.assert_reference_data_present")
    job = _job()
    job.boundary_geojson = None
    with pytest.raises(JobProcessingError):
        _orchestrator().process_job(job, AssessmentType.NUTRIENT)


def test_raises_when_assessment_produces_no_results(mocker):
    mocker.patch("app.orchestrator.assert_reference_data_present")
    mocker.patch.object(JobOrchestrator, "_process_inline_geometry", return_value={})
    with pytest.raises(JobProcessingError):
        _orchestrator().process_job(_job(), AssessmentType.NUTRIENT)


def test_returns_results_on_success(mocker):
    mocker.patch("app.orchestrator.assert_reference_data_present")
    dataframes = {"results": MagicMock()}
    mocker.patch.object(
        JobOrchestrator, "_process_inline_geometry", return_value=dataframes
    )
    mocker.patch.object(JobOrchestrator, "_send_results_callback")
    result = _orchestrator().process_job(_job(), AssessmentType.NUTRIENT)
    assert result is dataframes


def _bng_gdf(coords) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(geometry=[Polygon(coords)], crs="EPSG:27700")


class TestValidateGeodataframeBounds:
    """Coordinates are assumed to be EPSG:27700 metres. A lon/lat polygon is
    geometrically valid but intersects nothing, failing silently — the bounds
    check must reject it before the assessment runs."""

    def test_rejects_lonlat_degrees(self):
        gdf = _bng_gdf([(1.29, 52.67), (1.30, 52.67), (1.30, 52.68), (1.29, 52.68)])
        errors = _orchestrator()._validate_geodataframe(gdf)
        assert any("degrees" in e for e in errors)

    def test_rejects_coordinates_outside_bng_range(self):
        gdf = _bng_gdf(
            [
                (900_000, 328_188),
                (900_010, 328_188),
                (900_010, 328_198),
                (900_000, 328_198),
            ]
        )
        errors = _orchestrator()._validate_geodataframe(gdf)
        assert any("EPSG:27700" in e for e in errors)

    def test_accepts_valid_bng_polygon(self):
        gdf = _bng_gdf(
            [
                (582814.93, 328188.89),
                (582808.89, 328203.73),
                (582824.96, 328210.17),
                (582830.09, 328197.00),
            ]
        )
        assert _orchestrator()._validate_geodataframe(gdf) == []


def test_checks_reference_data_before_running_assessment(mocker):
    guard = mocker.patch("app.orchestrator.assert_reference_data_present")
    run = mocker.patch.object(
        JobOrchestrator, "_process_inline_geometry", return_value={"x": MagicMock()}
    )
    mocker.patch.object(JobOrchestrator, "_send_results_callback")
    repo = MagicMock()
    JobOrchestrator(MagicMock(), repo, MagicMock()).process_job(
        _job(), AssessmentType.NUTRIENT
    )
    guard.assert_called_once_with(repo, AssessmentType.NUTRIENT.value)
    run.assert_called_once()
