"""A quote the backend never received is a failed job, not a finished one.

A PATCH that still fails after the client's retries raises out of the
callback, so process_job raises and the consumer leaves the message on the
queue for redelivery / DLQ, as it does for a missing charge. A 200 stamps
sent_at on the audit row the quote was priced from, which tells "priced and
delivered" apart from "priced but never sent".
"""

import logging
from unittest.mock import MagicMock
from uuid import uuid4

import httpx
import pytest

from app.models.enums import AssessmentType
from app.models.job import BoundaryGeojson, ImpactAssessmentJob, IntersectingEdp
from app.orchestrator import JobOrchestrator, JobProcessingError, RecordedLevy
from tests.conftest import EDP_NAME, make_levy_calculation

AUDIT_ID = uuid4()
PAYLOAD = {"edps": [{"edpName": EDP_NAME}]}


def _job() -> ImpactAssessmentJob:
    return ImpactAssessmentJob(
        reference="NRL-000001",
        boundary_geojson=BoundaryGeojson(
            boundary_geometry_original={"type": "Polygon", "coordinates": []},
            intersecting_edps=[IntersectingEdp(label=EDP_NAME)],
        ),
    )


def _recorded() -> RecordedLevy:
    return RecordedLevy(levy=make_levy_calculation(), audit_id=AUDIT_ID)


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


@pytest.fixture
def mark_sent(mocker) -> MagicMock:
    return mocker.patch("app.orchestrator.mark_levy_calculation_sent")


def _send(orch: JobOrchestrator) -> None:
    orch._send_results_callback(_job(), {"impact_summary": MagicMock()}, _recorded())


@pytest.mark.parametrize(
    "error",
    [
        httpx.TransportError("connection refused"),
        httpx.HTTPStatusError(
            "503", request=MagicMock(), response=MagicMock(status_code=503)
        ),
    ],
)
def test_a_failed_patch_raises(orch, mark_sent, error):
    orch.backend_client.patch_quote.side_effect = error

    with pytest.raises(type(error)):
        _send(orch)

    mark_sent.assert_not_called()


def test_a_failure_building_the_payload_raises(orch, mark_sent, mocker):
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
def test_nothing_to_send_raises(orch, mark_sent, mocker, domain_results, payload):
    # Returning quietly would let process_job succeed and the consumer delete
    # the message with no quote sent.
    mocker.patch(
        "app.orchestrator.nutrient_adapter"
    ).to_domain_models.return_value = domain_results
    mocker.patch("app.orchestrator.build_quote_patch_payload", return_value=payload)

    with pytest.raises(JobProcessingError, match="NRL-000001"):
        _send(orch)

    orch.backend_client.patch_quote.assert_not_called()
    mark_sent.assert_not_called()


def test_a_delivered_quote_stamps_its_audit_row(orch, mark_sent):
    _send(orch)

    session = orch.repository.session.return_value.__enter__.return_value
    mark_sent.assert_called_once_with(session, AUDIT_ID)
    session.commit.assert_called_once()


def test_a_failed_stamp_after_delivery_does_not_raise(orch, mark_sent, caplog):
    # Raising here would redeliver the message and send the quote twice.
    mark_sent.side_effect = RuntimeError("database is down")

    with caplog.at_level(logging.ERROR, logger="app.orchestrator"):
        _send(orch)

    orch.backend_client.patch_quote.assert_called_once()
    assert any(
        "sent_at" in r.message and "NRL-000001" in r.message for r in caplog.records
    )


def test_process_job_raises_when_the_quote_is_not_delivered(orch, mocker):
    mocker.patch.object(JobOrchestrator, "_calculate_levy", return_value=_recorded())
    mocker.patch("app.orchestrator.assert_reference_data_present")
    mocker.patch.object(
        JobOrchestrator, "_process_inline_geometry", return_value={"x": MagicMock()}
    )
    orch.backend_client.patch_quote.side_effect = httpx.TransportError("down")
    job = _job()

    with pytest.raises(httpx.TransportError):
        orch.process_job(job, AssessmentType.NUTRIENT)


def test_process_job_raises_when_there_is_nothing_to_send(orch, mocker):
    mocker.patch.object(JobOrchestrator, "_calculate_levy", return_value=_recorded())
    mocker.patch("app.orchestrator.assert_reference_data_present")
    mocker.patch.object(
        JobOrchestrator, "_process_inline_geometry", return_value={"x": MagicMock()}
    )
    mocker.patch("app.orchestrator.build_quote_patch_payload", return_value=None)
    job = _job()

    with pytest.raises(JobProcessingError):
        orch.process_job(job, AssessmentType.NUTRIENT)

    orch.backend_client.patch_quote.assert_not_called()
