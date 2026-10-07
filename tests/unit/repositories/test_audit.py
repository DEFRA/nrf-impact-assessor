"""record_model_result writes one audit_model_results row per run: the
serialised output on success, the error on failure."""

from unittest.mock import MagicMock

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Point

from app.models.db import ModelResultRecord
from app.repositories.audit import record_model_result, serialise_model_output
from app.version.router import GIT_HASH


def test_serialise_drops_geometry_and_nulls_nan():
    gdf = gpd.GeoDataFrame(
        {"n_total": [np.float64(1.5), np.nan], "units": [np.int64(3), np.int64(4)]},
        geometry=[Point(0, 0), Point(1, 1)],
        crs="EPSG:27700",
    )
    out = serialise_model_output({"impact_summary": gdf, "other": pd.DataFrame()})
    assert out == {
        "impact_summary": [
            {"n_total": 1.5, "units": 3},
            {"n_total": None, "units": 4},
        ],
        "other": [],
    }


def test_success_row():
    session = MagicMock()
    df = pd.DataFrame({"a": [1]})
    row = record_model_result(session, "NRL-1", "nutrient", {"impact_summary": df})
    session.add.assert_called_once_with(row)
    assert isinstance(row, ModelResultRecord)
    assert row.status == "success"
    assert row.nrl_reference == "NRL-1"
    assert row.assessment_type == "nutrient"
    assert row.model_version == GIT_HASH
    assert row.model_output == {"impact_summary": [{"a": 1}]}
    assert row.error_details is None


def test_failed_row():
    session = MagicMock()
    row = record_model_result(
        session, "NRL-1", "gcn", None, ValueError("no reference data")
    )
    assert row.status == "failed"
    assert row.model_output is None
    assert row.error_details == "ValueError: no reference data"
