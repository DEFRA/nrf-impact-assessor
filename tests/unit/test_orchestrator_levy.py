"""The levy is the first step of the job and fails closed: a missing charge,
missing EDP_id, missing units, or an ambiguous (zero/multiple) EDP match
raises out of process_job before the geometry check, the reference-data guard
or the assessment run, so the SQS message stays on the queue and no PATCH
(and so no email) is sent."""

import logging
from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from app.calculators.levy import LevyCalculation, LevyChargeUnavailableError
from app.models.enums import AssessmentType
from app.models.job import BoundaryGeojson, ImpactAssessmentJob, IntersectingEdp
from app.orchestrator import JobOrchestrator
from tests.conftest import EDP_NAME


def _job(labels=(EDP_NAME,), units: int | None = 10) -> ImpactAssessmentJob:
    return ImpactAssessmentJob(
        reference="NRL-000001",
        boundary_geojson=BoundaryGeojson(
            boundary_geometry_original={"type": "Polygon", "coordinates": []},
            intersecting_edps=[IntersectingEdp(label=label) for label in labels],
        ),
        housingUnits=units,
    )


def _charge() -> MagicMock:
    charge = MagicMock()
    charge.id = uuid4()
    charge.edp_id = 1
    charge.edp_name = EDP_NAME
    charge.edp_start_date = date(2026, 1, 1)
    charge.base_charge_per_unit = Decimal("2193.6649")
    return charge


@pytest.fixture
def orch() -> JobOrchestrator:
    o = JobOrchestrator(MagicMock(), MagicMock(), MagicMock())
    o.repository.session.return_value.__enter__.return_value = MagicMock()
    return o


@pytest.fixture
def lookups(mocker) -> dict:
    return {
        "resolve": mocker.patch("app.orchestrator.resolve_edp_id", return_value=1),
        "charge": mocker.patch(
            "app.orchestrator.get_levy_charge", return_value=_charge()
        ),
        "index": mocker.patch("app.orchestrator.get_inflation_index"),
        "record": mocker.patch("app.orchestrator.record_levy_calculation"),
    }


def test_success_returns_calculation_and_logs_audit_record(orch, lookups, caplog):
    with caplog.at_level(logging.INFO, logger="app.orchestrator"):
        recorded = orch._calculate_levy(_job())

    levy = recorded.levy
    assert isinstance(levy, LevyCalculation)
    assert recorded.audit_id == lookups["record"].return_value.id
    assert levy.edp_id == 1
    assert levy.units == 10
    assert levy.provisional_amount == Decimal("21936.60")
    session = orch.repository.session.return_value.__enter__.return_value
    lookups["record"].assert_called_once_with(session, "NRL-000001", levy)
    session.commit.assert_called_once()
    # Same charging year: no index lookup needed.
    lookups["index"].assert_not_called()
    audit = next(r.message for r in caplog.records if "Levy calculated" in r.message)
    for fragment in (
        "quote=NRL-000001",
        "edp_id=1",
        "edp_start_date=2026-01-01",
        "calculator_version=1",
        "base_charge_per_unit=2193.6649",
        "units=10",
        "calculation_date=",
        "provisional_amount=21936.60",
        "inflation_adjusted_amount=21936.60",
        f"levy_charge_id={lookups['charge'].return_value.id}",
        "inflation_adjusted_charge_per_unit=None",
        "edp_start_year_index_id=None",
        "calculation_year_index_id=None",
    ):
        assert fragment in audit


def test_no_housing_units_raises(orch, lookups):
    job = _job(units=None)

    with pytest.raises(LevyChargeUnavailableError, match="no housing units"):
        orch._calculate_levy(job)


def test_no_edp_id_raises(orch, lookups):
    lookups["resolve"].return_value = None
    job = _job()

    with pytest.raises(LevyChargeUnavailableError, match="no EDP_id"):
        orch._calculate_levy(job)


def test_no_charge_row_raises(orch, lookups):
    lookups["charge"].return_value = None
    job = _job()

    with pytest.raises(LevyChargeUnavailableError, match="no charge"):
        orch._calculate_levy(job)


def test_applies_inflation_index_when_calculation_year_differs(orch, lookups):
    charge = _charge()
    charge.edp_start_date = date(2020, 1, 1)
    lookups["charge"].return_value = charge
    start = SimpleNamespace(id=uuid4(), index_factor=Decimal("300"))
    calc = SimpleNamespace(id=uuid4(), index_factor=Decimal("400"))
    lookups["index"].side_effect = [start, calc]

    levy = orch._calculate_levy(_job()).levy

    assert lookups["index"].call_count == 2
    assert levy.inflation_adjusted_amount != levy.provisional_amount
    # round_gbp(2193.6649 * 400 / 300) = 2924.89
    assert levy.inflation_adjusted_charge_per_unit == Decimal("2924.89")
    assert levy.edp_start_year_index_id == start.id
    assert levy.calculation_year_index_id == calc.id


def test_no_inflation_index_raises(orch, lookups):
    charge = _charge()
    charge.edp_start_date = date(2020, 1, 1)
    lookups["charge"].return_value = charge
    lookups["index"].return_value = None
    job = _job()

    with pytest.raises(LevyChargeUnavailableError, match="RICS CIL index"):
        orch._calculate_levy(job)
    lookups["record"].assert_not_called()


def test_zero_edps_raises_without_lookups(orch, lookups):
    job = _job(labels=())

    with pytest.raises(LevyChargeUnavailableError, match="no EDP"):
        orch._calculate_levy(job)
    lookups["resolve"].assert_not_called()
    lookups["record"].assert_not_called()


def test_multiple_edps_raises_without_lookups(orch, lookups):
    job = _job(labels=(EDP_NAME, "Other EDP"))

    with pytest.raises(LevyChargeUnavailableError, match="multiple EDPs"):
        orch._calculate_levy(job)
    lookups["resolve"].assert_not_called()
    lookups["record"].assert_not_called()


def test_missing_boundary_returns_none(orch, lookups):
    job = ImpactAssessmentJob(reference="NRL-000001", housingUnits=10)

    assert orch._calculate_levy(job) is None
    lookups["resolve"].assert_not_called()


def test_failure_does_not_record_an_audit_row(orch, lookups):
    lookups["charge"].return_value = None
    job = _job()

    with pytest.raises(LevyChargeUnavailableError):
        orch._calculate_levy(job)

    lookups["record"].assert_not_called()


def test_process_job_fails_before_anything_else_runs(orch, lookups, mocker, caplog):
    lookups["charge"].return_value = None
    guard = mocker.patch("app.orchestrator.assert_reference_data_present")
    run = mocker.patch.object(JobOrchestrator, "_process_inline_geometry")
    callback = mocker.patch.object(JobOrchestrator, "_send_results_callback")
    job = _job()

    with (
        caplog.at_level(logging.ERROR, logger="app.orchestrator"),
        pytest.raises(LevyChargeUnavailableError),
    ):
        orch.process_job(job, AssessmentType.NUTRIENT)

    guard.assert_not_called()
    run.assert_not_called()
    callback.assert_not_called()
    assert any("no charge" in r.message for r in caplog.records)


def test_process_job_passes_the_levy_to_the_callback(orch, lookups, mocker):
    mocker.patch("app.orchestrator.assert_reference_data_present")
    dataframes = {"results": MagicMock()}
    mocker.patch.object(
        JobOrchestrator, "_process_inline_geometry", return_value=dataframes
    )
    callback = mocker.patch.object(JobOrchestrator, "_send_results_callback")

    orch.process_job(_job(), AssessmentType.NUTRIENT)

    job_arg, frames_arg, recorded_arg = callback.call_args.args
    assert frames_arg is dataframes
    assert isinstance(recorded_arg.levy, LevyCalculation)
    assert recorded_arg.audit_id == lookups["record"].return_value.id
