"""A quote the backend never received is a failed job, not a finished one.

A PATCH that still fails after the client's retries raises out of the
callback, so process_job raises and the consumer leaves the message on the
queue for redelivery / DLQ, as it does for a missing charge.
"""

from unittest.mock import MagicMock

import httpx
import pytest

from app.models.enums import AssessmentType
from app.models.job import BoundaryGeojson, ImpactAssessmentJob, IntersectingEdp
from app.orchestrator import JobOrchestrator, JobProcessingError, PricedLevy
from tests.conftest import CALCULATED_AT, EDP_NAME, make_levy_calculation

PAYLOAD = {"edps": [{"edpName": EDP_NAME}]}


def _job() -> ImpactAssessmentJob:
    return ImpactAssessmentJob(
        reference="NRL-000001",
        boundary_geojson=BoundaryGeojson(
            boundary_geometry_original={"type": "Polygon", "coordinates": []},
            intersecting_edps=[IntersectingEdp(label=EDP_NAME)],
        ),
    )


def _priced() -> PricedLevy:
    return PricedLevy(levy=make_levy_calculation(), calculated_at=CALCULATED_AT)


@pytest.fixture
def orch(mocker) -> JobOrchestrator:
    o = JobOrchestrator.__new__(JobOrchestrator)
    o.repository = MagicMock()
    o.backend_client = MagicMock()
    mocker.patch("app.orchestrator.resolve_active_provenance", return_value=None)
    adapter = mocker.patch("app.orchestrator.nutrient_adapter")
    adapter.to_domain_models.return_value = {
        "assessment_results": [MagicMock(catchment_impacts=[MagicMock()])]
    }
    mocker.patch.object(JobOrchestrator, "_boundary_catchments", return_value=[])
    mocker.patch("app.orchestrator.build_quote_patch_payload", return_value=PAYLOAD)
    return o


def _send(orch: JobOrchestrator) -> None:
    orch._send_results_callback(_job(), {"impact_summary": MagicMock()}, _priced())


@pytest.mark.parametrize(
    "error",
    [
        httpx.TransportError("connection refused"),
        httpx.HTTPStatusError(
            "503", request=MagicMock(), response=MagicMock(status_code=503)
        ),
    ],
)
def test_a_failed_patch_raises(orch, error):
    orch.backend_client.patch_quote.side_effect = error

    with pytest.raises(type(error)):
        _send(orch)


def test_a_failure_building_the_payload_raises(orch, mocker):
    mocker.patch(
        "app.orchestrator.build_quote_patch_payload",
        side_effect=ValueError("bad levy"),
    )

    with pytest.raises(ValueError, match="bad levy"):
        _send(orch)

    orch.backend_client.patch_quote.assert_not_called()


@pytest.mark.parametrize(
    ("domain_results", "payload"),
    [
        pytest.param({"assessment_results": []}, PAYLOAD, id="no-results"),
        pytest.param(
            {"assessment_results": [MagicMock(catchment_impacts=[])]},
            PAYLOAD,
            id="no-catchment-impacts",
        ),
        pytest.param(
            {"assessment_results": [MagicMock(catchment_impacts=[MagicMock()])]},
            None,
            id="no-payload",
        ),
    ],
)
def test_nothing_to_send_raises(orch, mocker, domain_results, payload):
    # Returning quietly would let process_job succeed and the consumer delete
    # the message with no quote sent.
    mocker.patch(
        "app.orchestrator.nutrient_adapter"
    ).to_domain_models.return_value = domain_results
    mocker.patch("app.orchestrator.build_quote_patch_payload", return_value=payload)

    with pytest.raises(JobProcessingError, match="NRL-000001"):
        _send(orch)

    orch.backend_client.patch_quote.assert_not_called()


def test_process_job_raises_when_the_quote_is_not_delivered(orch, mocker):
    mocker.patch.object(JobOrchestrator, "_calculate_levy", return_value=_priced())
    mocker.patch("app.orchestrator.assert_reference_data_present")
    mocker.patch.object(
        JobOrchestrator, "_process_inline_geometry", return_value={"x": MagicMock()}
    )
    orch.backend_client.patch_quote.side_effect = httpx.TransportError("down")
    job = _job()

    with pytest.raises(httpx.TransportError):
        orch.process_job(job, AssessmentType.NUTRIENT)


def test_process_job_raises_when_there_is_nothing_to_send(orch, mocker):
    mocker.patch.object(JobOrchestrator, "_calculate_levy", return_value=_priced())
    mocker.patch("app.orchestrator.assert_reference_data_present")
    mocker.patch.object(
        JobOrchestrator, "_process_inline_geometry", return_value={"x": MagicMock()}
    )
    mocker.patch("app.orchestrator.build_quote_patch_payload", return_value=None)
    job = _job()

    with pytest.raises(JobProcessingError):
        orch.process_job(job, AssessmentType.NUTRIENT)

    orch.backend_client.patch_quote.assert_not_called()
