"""The full nutrient pipeline runs against the committed EDP fixture data.

Loads the nutrient reference layers, the blanket coefficient layer and the
lookup tables from tests/data/fixtures/ into PostGIS, then runs every nutrient
input in tests/data/inputs/generated/manifest.json through the runner.

This guards the fixtures themselves: the assertions are about coverage and
pipeline completion (a WwTW is assigned, the catchment area is measured, the
lookups join), not about exact nutrient loads. The fixture coefficient layer is
a single blanket polygon, so the numbers are deliberately not production values
— see tests/regression/ for value-level checks against the real database.
"""

import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from load_data import SpatialDataLoader
from sqlalchemy import text

from app.assessments import nutrient
from app.repositories.repository import Repository
from app.runner.runner import run_assessment

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).resolve().parents[3]
FIXTURES_DIR = PROJECT_ROOT / "tests" / "data" / "fixtures"
GENERATED_DIR = PROJECT_ROOT / "tests" / "data" / "inputs" / "generated"
MANIFEST = GENERATED_DIR / "manifest.json"

# Layers the nutrient assessment reads. The GCN layers are skipped: they are
# large and irrelevant here.
NUTRIENT_LAYERS = [
    "wwtw_catchments",
    "lpa_boundaries",
    "nn_catchments",
    "subcatchments",
]

# 01_tiny_parcel sits outside every NN catchment and WwTW catchment in the
# fixtures, so _filter_out_of_scope drops it. Verified to behave the same way
# against the full production dataset.
OUT_OF_SCOPE = {"01_tiny_parcel"}

# 08_small_east falls outside the NN catchments but inside a WwTW catchment, so
# it survives the out-of-scope filter with a null catchment area. Also verified
# against production.
NO_NN_CATCHMENT = {"08_small_east"}


def _nutrient_manifest_entries() -> list[dict]:
    entries = json.loads(MANIFEST.read_text())
    return [e for e in entries if e["assessment_type"] == "nutrient"]


def _rlb_from_entry(entry: dict) -> gpd.GeoDataFrame:
    """Build an assessment input from a manifest entry's geometry file."""
    gdf = gpd.read_file(PROJECT_ROOT / entry["geometry"])
    gdf["id"] = range(1, len(gdf) + 1)
    gdf["name"] = entry["name"]
    gdf["dwelling_category"] = entry["dwelling_type"]
    gdf["source"] = "manifest"
    gdf["dwellings"] = entry["dwellings"]
    gdf["shape_area"] = gdf.geometry.area
    return gdf


# Truncated on teardown as well as by the `repository` fixture on setup. The
# setup truncate is what guarantees isolation; this is defence in depth, because
# these layers are large enough to change how other tests behave — the data_sync
# QC gate's row_count rule reads public.<table> and only applies its percentage
# floor above 10 live rows, and 170 loaded catchments clear that easily.
_LOADED_TABLES = (
    "public.wwtw_catchments",
    "public.lpa_boundaries",
    "public.nn_catchments",
    "public.subcatchments",
    "public.coefficient_layer",
    "public.lookup_table",
)


@pytest.fixture
def nutrient_fixture_repository(repository: Repository):
    """Load the nutrient reference data from tests/data/fixtures/ into PostGIS."""
    # The lookup cache is process-level and keyed by (table, version); a
    # previous test's data would otherwise be reused at the same version.
    nutrient._lookup_cache.clear()

    loader = SpatialDataLoader(repository, fixtures_dir=FIXTURES_DIR)
    loader.load_spatial_layers(layer_types=NUTRIENT_LAYERS)
    loader.load_coefficient_layer()
    loader.load_lookup_tables()

    yield repository

    nutrient._lookup_cache.clear()
    with repository.engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(text(f"TRUNCATE {', '.join(_LOADED_TABLES)} CASCADE"))


@pytest.mark.parametrize(
    "entry",
    _nutrient_manifest_entries(),
    ids=lambda e: Path(e["geometry"]).stem,
)
def test_nutrient_runs_on_generated_edp_input(
    entry: dict, nutrient_fixture_repository: Repository
):
    """Every generated nutrient input completes the full pipeline."""
    stem = Path(entry["geometry"]).stem
    rlb_gdf = _rlb_from_entry(entry)

    results = run_assessment(
        "nutrient",
        rlb_gdf,
        {"unique_ref": f"fixtures_{stem}"},
        nutrient_fixture_repository,
    )

    df = results["impact_summary"]

    if stem in OUT_OF_SCOPE:
        assert df.empty, f"{stem} is expected to be filtered out of scope"
        return

    assert len(df) == 1, f"{stem} should produce one row per RLB"
    row = df.iloc[0]

    # Spatial assignment reached every layer the pipeline needs.
    assert pd.notna(row["majority_wwtw_id"]), "no WwTW catchment assigned"
    assert pd.notna(row["majority_opcat_name"]), "no subcatchment assigned"

    # WwTW lookup join succeeded, so the fixture lookups cover the assigned WwTW.
    assert isinstance(row["wwtw_name"], str)
    assert row["wwtw_name"], "WwTW lookup did not resolve a name"

    # Totals are computed rather than left null.
    assert pd.notna(row["n_total"]), "n_total is null"
    assert pd.notna(row["p_total"]), "p_total is null"
    assert row["n_total"] >= 0
    assert row["p_total"] >= 0

    if stem in NO_NN_CATCHMENT:
        # The rates lookup joins on the NN catchment, so it cannot resolve here.
        assert pd.isna(row["area_in_nn_catchment_ha"]), (
            f"{stem} is expected to fall outside the NN catchments"
        )
        return

    # Coefficient layer and NN catchments both intersected the site.
    assert row["area_in_nn_catchment_ha"] > 0
    assert row["nn_catchment"], "no NN catchment name resolved"
    assert pd.notna(row["n_lu_uplift"]), "nitrogen land use uplift is null"
    assert pd.notna(row["p_lu_uplift"]), "phosphorus land use uplift is null"

    # Rates lookup joined, so the fixture lookups cover the assigned catchment.
    assert row["occupancy_rate"] > 0
    assert row["daily_water_usage_L"] > 0

    # Wastewater loads were actually computed, not defaulted to zero.
    assert row["n_wwtw_perm"] > 0
    assert row["p_wwtw_perm"] > 0
