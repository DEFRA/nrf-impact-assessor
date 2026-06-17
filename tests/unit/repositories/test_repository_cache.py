from datetime import UTC, datetime
from unittest.mock import MagicMock

from app.models.db import GcnRiskZones
from app.repositories.repository import (
    _intersection_cache_key,
    _land_use_cache_key,
    _spatial_cache_generation,
)


def test_spatial_cache_generation_uses_latest_successful_data_load():
    session = MagicMock()
    loaded_at = datetime(2026, 6, 16, 12, 30, tzinfo=UTC)
    session.scalar.return_value = loaded_at

    assert _spatial_cache_generation(session) == loaded_at.isoformat()


def test_spatial_cache_generation_has_stable_sentinel_without_data_loads():
    session = MagicMock()
    session.scalar.return_value = None

    assert _spatial_cache_generation(session) == "no-successful-data-load"


def test_land_use_cache_key_changes_when_data_load_generation_changes():
    gdf = MagicMock()
    gdf.to_dict.return_value = [
        {
            "rlb_id": 1,
            "dwellings": 10,
            "name": "NRF-1",
            "dwelling_category": "housing",
            "source": "web_submission",
        }
    ]
    gdf.geometry.to_wkt.return_value = ["POLYGON ((0 0, 1 0, 1 1, 0 0))"]

    first = _land_use_cache_key(gdf, coeff_version=1, nn_version=1, generation="run-a")
    second = _land_use_cache_key(gdf, coeff_version=1, nn_version=1, generation="run-b")

    assert first != second


def test_intersection_cache_key_changes_when_data_load_generation_changes():
    first = _intersection_cache_key(
        input_wkt="POLYGON ((0 0, 1 0, 1 1, 0 0))",
        overlay_table=GcnRiskZones,
        filter_str="public.gcn_risk_zones.version >= 1",
        overlay_columns=[],
        json_extracts={"attributes": ["RZ"]},
        generation="run-a",
    )
    second = _intersection_cache_key(
        input_wkt="POLYGON ((0 0, 1 0, 1 1, 0 0))",
        overlay_table=GcnRiskZones,
        filter_str="public.gcn_risk_zones.version >= 1",
        overlay_columns=[],
        json_extracts={"attributes": ["RZ"]},
        generation="run-b",
    )

    assert first != second
