"""Regression test fixtures.

Fixtures for regression tests that use the full production database.
"""

from pathlib import Path

import pytest
from sqlalchemy import create_engine

from app.config import DatabaseSettings
from app.repositories.repository import Repository

# Maps internal DataFrame column names to the legacy baseline CSV column names.
# Kept in sync between the regression tests and the baseline update script.
INTERNAL_TO_BASELINE_COLUMNS = {
    "rlb_id": "RLB_ID",
    "name": "Name",
    "dwelling_category": "Dwel_Cat",
    "source": "Source",
    "dwellings": "Dwellings",
    "dev_area_ha": "Dev_Area_Ha",
    "area_in_nn_catchment_ha": "AreaInNNCatchment",
    "nn_catchment": "NN_Catchment",
    "majority_opcat_name": "Dev_SubCatchment",
    "majority_name": "Majority_LPA",
    "majority_wwtw_id": "Majority_WwTw_ID",
    "wwtw_name": "WwTW_name",
    "wwtw_subcatchment": "WwTw_SubCatchment",
    "n_lu_uplift": "N_LU_Uplift",
    "p_lu_uplift": "P_LU_Uplift",
    "n_lu_post_suds": "N_LU_postSuDS",
    "p_lu_post_suds": "P_LU_postSuDS",
    "occupancy_rate": "Occ_Rate",
    "water_usage_L_per_person_day": "Water_Usage_L_Day",
    "daily_water_usage_L": "Litres_used",
    "nitrogen_conc_2025_2030_mg_L": "Nitrogen_2025_2030",
    "nitrogen_conc_2030_onwards_mg_L": "Nitrogen_2030_onwards",
    "phosphorus_conc_2025_2030_mg_L": "Phosphorus_2025_2030",
    "phosphorus_conc_2030_onwards_mg_L": "Phosphorus_2030_onwards",
    "n_wwtw_temp": "N_WwTW_Temp",
    "p_wwtw_temp": "P_WwTW_Temp",
    "n_wwtw_perm": "N_WwTW_Perm",
    "p_wwtw_perm": "P_WwTW_Perm",
    "n_total": "N_Total",
    "p_total": "P_Total",
}


@pytest.fixture
def test_data_dir() -> Path:
    """Path to test data directory.

    Returns:
        Path to tests/data directory
    """
    return Path(__file__).parent.parent / "data"


@pytest.fixture
def production_repository() -> Repository:
    """Repository connected to production nrf_impact database.

    Requires:
    - PostgreSQL running with PostGIS
    - Database migrations applied
    - Full reference data loaded via scripts/load_data.py

    Connection is configured via DB_* environment variables (same as the app).
    Defaults: host=localhost, port=5432, database=nrf_impact, user=postgres.

    Returns:
        Repository instance connected to nrf_impact database
    """
    settings = DatabaseSettings()
    engine = create_engine(settings.connection_url)
    return Repository(engine)


@pytest.fixture
def tolerance() -> dict[str, float]:
    """Numerical tolerance for comparing outputs.

    Returns:
        Dictionary of tolerances for different value types
    """
    return {
        "absolute": 0.01,  # kg/year - allow 0.01 kg difference
        "relative": 0.02,  # 2% relative difference (PostGIS ST_Area vs Python geometry.area)
    }
