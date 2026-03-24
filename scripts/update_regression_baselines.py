"""Regenerate nutrient regression baseline CSVs from the production PostGIS database.

Run via:
    make update-regression-baseline

Each baseline is produced by running the full assessment pipeline against the
production DB and saving the output using the same column names as the legacy
script.  Commit the updated CSVs to lock in the new PostGIS ground truth.
"""

import sys
from pathlib import Path

import geopandas as gpd
from sqlalchemy import create_engine

# Allow imports from the project root
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import DatabaseSettings
from app.repositories.repository import Repository
from app.runner.runner import run_assessment

TESTS_DIR = Path(__file__).parent.parent / "tests"
INPUTS_DIR = TESTS_DIR / "data" / "inputs" / "nutrients"
EXPECTED_DIR = TESTS_DIR / "data" / "expected" / "nutrients"

# Internal column name → baseline CSV column name (kept in sync with the regression test)
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

BASELINES = [
    {
        "name": "BnW_small_under_1_hectare",
        "input": INPUTS_DIR / "BnW_small_under_1_hectare" / "BnW_small_under_1_hectare.shp",
        "output": EXPECTED_DIR / "BnW_small_under_1_hectare.csv",
        "unique_ref": "update_baseline_small",
    },
    {
        "name": "BroadsWensum_IAT_101025",
        "input": INPUTS_DIR / "BnW_cleaned_110925" / "BnW_cleaned_110925.shp",
        "output": EXPECTED_DIR / "BroadsWensum_IAT_101025.csv",
        "unique_ref": "update_baseline_full",
    },
]


def main() -> None:
    settings = DatabaseSettings()
    engine = create_engine(settings.connection_url)
    repository = Repository(engine)

    for baseline in BASELINES:
        input_path: Path = baseline["input"]
        output_path: Path = baseline["output"]

        if not input_path.exists():
            print(f"  SKIP  {baseline['name']} — input not found: {input_path}")
            continue

        print(f"  Running {baseline['name']}...")
        gdf = gpd.read_file(input_path)
        dataframes = run_assessment(
            assessment_type="nutrient",
            rlb_gdf=gdf,
            metadata={"unique_ref": baseline["unique_ref"]},
            repository=repository,
        )

        df = dataframes["impact_summary"].rename(columns=INTERNAL_TO_BASELINE_COLUMNS)
        df.to_csv(output_path, index=False)
        print(f"  OK    {output_path.relative_to(Path(__file__).parent.parent)} ({len(df)} rows)")

    print("\nDone. Review the diff and commit to lock in the new PostGIS baseline.")


if __name__ == "__main__":
    main()
