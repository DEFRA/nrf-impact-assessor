"""The PATCH callback must name EDPs from the job, not from NN catchments.

`edpName` used to carry the NN catchment's N2K site name ("River Wensum SAC"),
which is a constituent site of an EDP rather than the EDP itself. The EDP name
arrives on the job as `intersectingEdps[].label`.
"""

from unittest.mock import MagicMock, patch

from app.models.domain import (
    CatchmentImpact,
    Development,
    ImpactAssessmentResult,
    LandUseImpact,
    NutrientImpact,
    SpatialAssignment,
)
from app.models.job import BoundaryGeojson, ImpactAssessmentJob, IntersectingEdp
from app.orchestrator import JobOrchestrator

EDP_LABEL = "Broads SAC (Yare & Bure) & Wensum SAC"


def _result() -> ImpactAssessmentResult:
    return ImpactAssessmentResult(
        rlb_id=1,
        development=Development(
            id="TEST-001",
            name="Test Development",
            dwelling_category="house",
            source="test",
            dwellings=1,
            area_m2=1000.0,
            area_ha=0.1,
        ),
        spatial=SpatialAssignment(
            wwtw_id=1,
            wwtw_name="Test WwTW",
            lpa_name="Test LPA",
            area_in_nn_catchment_ha=0.1,
        ),
        land_use=LandUseImpact(nitrogen_kg_yr=5.0, phosphorus_kg_yr=1.0),
        total=NutrientImpact(nitrogen_total_kg_yr=20.0, phosphorus_total_kg_yr=2.0),
        catchment_impacts=[
            CatchmentImpact(
                catchment_id="10",
                catchment_name="River Wensum SAC",
                nitrogen_total_kg_yr=20.0,
                phosphorus_total_kg_yr=2.0,
            )
        ],
    )


def _job(labels: list[str]) -> ImpactAssessmentJob:
    return ImpactAssessmentJob(
        reference="NRL-000001",
        boundary_geojson=BoundaryGeojson(
            boundary_geometry_original={"type": "Polygon", "coordinates": []},
            intersecting_edps=[IntersectingEdp(label=label) for label in labels],
        ),
    )


def _run_callback(job: ImpactAssessmentJob, catchments=None) -> MagicMock:
    orch = JobOrchestrator.__new__(JobOrchestrator)
    orch.repository = MagicMock()
    orch.backend_client = MagicMock()

    with (
        patch("app.orchestrator.resolve_active_provenance", return_value=None),
        patch("app.orchestrator.nutrient_adapter") as adapter,
        patch(
            "app.orchestrator.find_intersecting_catchments",
            return_value=catchments if catchments is not None else [],
        ),
    ):
        adapter.to_domain_models.return_value = {"assessment_results": [_result()]}
        orch._send_results_callback(job, {"impact_summary": MagicMock()})

    return orch.backend_client


def test_callback_names_edp_from_job_label():
    client = _run_callback(_job([EDP_LABEL]))

    client.patch_quote.assert_called_once()
    payload = client.patch_quote.call_args.args[1]
    assert [edp["edpName"] for edp in payload["edps"]] == [EDP_LABEL]


def test_callback_skipped_when_job_has_no_edps():
    client = _run_callback(_job([]))

    client.patch_quote.assert_not_called()


CATCHMENTS = [
    {
        "label": "River Wensum SAC",
        "catchmentId": "10",
        "catchmentOverlapPercentage": 32.6,
    }
]


def test_callback_carries_the_recomputed_catchments():
    client = _run_callback(_job([EDP_LABEL]), catchments=CATCHMENTS)

    payload = client.patch_quote.call_args.args[1]
    assert payload["edps"][0]["catchments"] == CATCHMENTS


def test_callback_is_still_sent_when_the_catchment_query_fails():
    """The assessment is the payload's reason to exist; catchments decorate it."""
    orch = JobOrchestrator.__new__(JobOrchestrator)
    orch.repository = MagicMock()
    orch.backend_client = MagicMock()

    with (
        patch("app.orchestrator.resolve_active_provenance", return_value=None),
        patch("app.orchestrator.nutrient_adapter") as adapter,
        patch(
            "app.orchestrator.find_intersecting_catchments",
            side_effect=RuntimeError("PostGIS is down"),
        ),
    ):
        adapter.to_domain_models.return_value = {"assessment_results": [_result()]}
        orch._send_results_callback(_job([EDP_LABEL]), {"impact_summary": MagicMock()})

    payload = orch.backend_client.patch_quote.call_args.args[1]
    assert payload["edps"][0]["catchments"] == []
