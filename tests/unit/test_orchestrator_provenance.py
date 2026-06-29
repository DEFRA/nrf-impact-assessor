"""Orchestrator resolves and carries reference-data provenance (DM-3)."""

from unittest.mock import MagicMock, patch

from app.models.domain import DataProvenance
from app.orchestrator import JobOrchestrator


def test_callback_resolves_and_passes_provenance():
    orch = JobOrchestrator.__new__(JobOrchestrator)
    orch.repository = MagicMock()
    orch.backend_client = MagicMock()
    prov = DataProvenance(data_version="2026.07.01")

    job = MagicMock(reference="Q1")
    with (
        patch(
            "app.orchestrator.resolve_active_provenance", return_value=prov
        ) as resolve,
        patch("app.orchestrator.nutrient_adapter") as adapter,
    ):
        adapter.to_domain_models.return_value = {"assessment_results": []}
        orch._send_results_callback(job, {"impact_summary": MagicMock()})

    resolve.assert_called_once()
    assert adapter.to_domain_models.call_args.kwargs["provenance"] == prov
