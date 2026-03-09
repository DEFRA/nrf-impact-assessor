"""Unit tests for POST /test/assess (WKT direct assessment endpoint)."""

from unittest.mock import MagicMock, patch

import geopandas as gpd
import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from shapely.geometry import Polygon

from app.test.router import router

# Build a minimal app with just the test router so tests are isolated
# from the testing_enabled flag in main.py.
_app = FastAPI()
_app.include_router(router, prefix="/test")
client = TestClient(_app)

# A simple square polygon in EPSG:27700 (British National Grid)
_POLYGON_BNG = "POLYGON ((450000 100000, 450500 100000, 450500 100500, 450000 100500, 450000 100000))"

_MOCK_RESULTS = {"nutrient_results": [{"RLB_ID": 1, "N_Total": 10.5, "P_Total": 1.2}]}


def _make_mock_dataframes():
    return {
        "nutrient_results": pd.DataFrame(
            [{"RLB_ID": 1, "N_Total": 10.5, "P_Total": 1.2}]
        )
    }


@pytest.fixture(autouse=True)
def mock_repository():
    """Mock the repository so tests never need a real DB connection."""
    with patch("app.test.router._get_repository") as mock:
        mock.return_value = MagicMock()
        yield mock


@pytest.fixture(autouse=True)
def mock_run_assessment():
    """Mock run_assessment so tests exercise the router without real assessments."""
    with patch("app.test.router.run_assessment") as mock:
        mock.return_value = _make_mock_dataframes()
        yield mock


class TestWktAssessEndpoint:
    def test_valid_request_returns_200(self):
        response = client.post(
            "/test/assess",
            json={"wkt": _POLYGON_BNG, "assessment_type": "nutrient"},
        )
        assert response.status_code == 200

    def test_response_schema(self):
        response = client.post(
            "/test/assess",
            json={"wkt": _POLYGON_BNG, "assessment_type": "nutrient"},
        )
        body = response.json()
        assert "job_id" in body
        assert body["assessment_type"] == "nutrient"
        assert isinstance(body["timing_s"], float)
        assert "results" in body
        assert "nutrient_results" in body["results"]

    def test_run_assessment_called_with_correct_type(self, mock_run_assessment):
        client.post(
            "/test/assess",
            json={"wkt": _POLYGON_BNG, "assessment_type": "gcn"},
        )
        call_kwargs = mock_run_assessment.call_args
        assert call_kwargs.kwargs["assessment_type"] == "gcn"

    def test_job_fields_injected_into_gdf(self, mock_run_assessment):
        client.post(
            "/test/assess",
            json={
                "wkt": _POLYGON_BNG,
                "assessment_type": "nutrient",
                "dwelling_type": "apartment",
                "dwellings": 42,
                "name": "Test Site",
            },
        )
        gdf: gpd.GeoDataFrame = mock_run_assessment.call_args.kwargs["rlb_gdf"]
        assert (gdf["dwelling_category"] == "apartment").all()
        assert (gdf["dwellings"] == 42).all()
        assert (gdf["name"] == "Test Site").all()

    def test_gdf_reprojected_to_bng_when_wgs84_input(self, mock_run_assessment):
        wkt_wgs84 = "POLYGON ((-1.5 52.5, -1.4 52.5, -1.4 52.6, -1.5 52.6, -1.5 52.5))"
        response = client.post(
            "/test/assess",
            json={"wkt": wkt_wgs84, "crs": "EPSG:4326", "assessment_type": "nutrient"},
        )
        assert response.status_code == 200
        gdf: gpd.GeoDataFrame = mock_run_assessment.call_args.kwargs["rlb_gdf"]
        assert gdf.crs.to_epsg() == 27700

    def test_invalid_assessment_type_returns_400(self):
        response = client.post(
            "/test/assess",
            json={"wkt": _POLYGON_BNG, "assessment_type": "unknown"},
        )
        assert response.status_code == 400
        assert "assessment_type" in response.json()["detail"]

    def test_invalid_wkt_returns_400(self):
        response = client.post(
            "/test/assess",
            json={"wkt": "NOT VALID WKT", "assessment_type": "nutrient"},
        )
        assert response.status_code == 400
        assert "Invalid WKT" in response.json()["detail"]

    def test_assessment_failure_returns_500(self, mock_run_assessment):
        mock_run_assessment.side_effect = ValueError("assessment exploded")
        response = client.post(
            "/test/assess",
            json={"wkt": _POLYGON_BNG, "assessment_type": "nutrient"},
        )
        assert response.status_code == 500
        assert "assessment exploded" in response.json()["detail"]

    def test_geometry_column_dropped_from_results(self):
        """GeoDataFrame results must have their geometry column stripped before serialisation."""
        poly = Polygon(
            [(450000, 100000), (450500, 100000), (450500, 100500), (450000, 100500)]
        )
        geo_df = gpd.GeoDataFrame(
            [{"RLB_ID": 1, "N_Total": 5.0}], geometry=[poly], crs="EPSG:27700"
        )
        with patch("app.test.router.run_assessment", return_value={"results": geo_df}):
            response = client.post(
                "/test/assess",
                json={"wkt": _POLYGON_BNG, "assessment_type": "nutrient"},
            )
        assert response.status_code == 200
        records = response.json()["results"]["results"]
        assert all("geometry" not in r for r in records)

    def test_default_fields(self, mock_run_assessment):
        """Defaults: crs=EPSG:27700, dwelling_type=house, dwellings=1, name=''."""
        client.post("/test/assess", json={"wkt": _POLYGON_BNG})
        gdf: gpd.GeoDataFrame = mock_run_assessment.call_args.kwargs["rlb_gdf"]
        assert (gdf["dwelling_category"] == "house").all()
        assert (gdf["dwellings"] == 1).all()
        assert (gdf["name"] == "").all()
