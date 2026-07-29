"""Generate a single-polygon coefficient_layer.gpkg test fixture.

Run: uv run python scripts/make_coefficient_layer_fixture.py

The real coefficient layer is ~5.4M polygons nationally; clipped to the EDP it
is still ~563k features (~281MB), too large to commit. This fixture replaces it
with one polygon blanketing the EDP boundary and the excluded areas, so any test
parcel inside the EDP resolves to a coefficient.

The attributes are the full, unmodified attribute set of a real coefficient
polygon — the most common coefficient profile in the layer (RESIDENTIAL URBAN
LAND / THE BROADS SAC / YARE, 5,670 of 111,898 polygons). Taking one real row
whole keeps the values internally consistent, satisfies the coefficient_ranges
and referential checks in app/data_sync/qc_rules.yaml, and keeps the fixture
schema identical to the source layer.

Because coverage is uniform, assessments run against this fixture no longer
exercise coefficient-to-parcel assignment: every parcel gets the same
coefficients wherever it sits.
"""

from pathlib import Path

import geopandas as gpd
import shapely
from shapely.geometry import MultiPolygon, Polygon
from shapely.ops import unary_union

_FIXTURES = Path(__file__).parent.parent / "tests" / "data" / "fixtures"
OUT = _FIXTURES / "coefficient_layer.gpkg"
LAYER = "coefficient_layer"

# Slack for floating-point noise when checking the blanket covers its inputs.
_COVERAGE_TOLERANCE_M2 = 1.0

# Verbatim attributes of coefficient polygon RPA619486299391.
ATTRIBUTES = {
    "cromeid": "RPA619486299391",
    "AvgLuRes": "NA01",
    "Urban_check": "TRUE",
    "Urban_open_check": "FALSE",
    "June_Ag": "Grazing",
    "RPA_check": "FALSE",
    "Land_use_cat": "RESIDENTIAL URBAN LAND",
    "NN_Catchment": "THE BROADS SAC",
    "SubCatchment": "YARE",
    "NVZ_check": 0,
    "major_soilscape": (
        "Slowly permeable seasonally wet slightly acid but base-rich loamy "
        "and clayey soils"
    ),
    "Soil_category": "DRAINEDARGR",
    "Rainfall_value": 676.9270088710244,
    "Rain_Band": "675.1 - 700",
    "LU_CurrNcoeff": "12.79",
    "LU_CurrPcoeff": "1.37",
    "Match_Source": "Land Cover + Rainfall",
    "ResiRainfallBand": "675.1 - 700",
    "N_ResiCoeff": "12.79",
    "P_ResiCoeff": "1.37",
}


def _fill_holes(geom: MultiPolygon | Polygon) -> MultiPolygon:
    """Drop interior rings so the blanket has no gaps.

    The EDP boundary carries 12 interior holes; a parcel landing in one would
    otherwise resolve to no coefficient at all. Filling them can leave parts
    overlapping — some are islands sitting inside another part's hole — so the
    filled parts are dissolved back together.
    """
    parts = geom.geoms if hasattr(geom, "geoms") else [geom]
    filled = unary_union([Polygon(part.exterior) for part in parts])
    return filled if isinstance(filled, MultiPolygon) else MultiPolygon([filled])


def main() -> None:
    boundary = gpd.read_file(_FIXTURES / "edp_boundary_extents.gpkg")
    excluded = gpd.read_file(_FIXTURES / "edp_excluded_areas.gpkg")

    # The EDP boundary carries Z; load_data drops it at load time, so drop it
    # here too and keep the fixture 2D like the geometry it will become in PostGIS.
    geometries = [
        shapely.force_2d(geom)
        for geom in list(boundary.geometry) + list(excluded.geometry)
    ]
    blanket = _fill_holes(unary_union(geometries))

    # Measured as leftover area rather than with covers(): the repaired boundary
    # makes the predicate report False even where nothing is actually left out.
    uncovered = sum(geom.difference(blanket).area for geom in geometries)
    if uncovered > _COVERAGE_TOLERANCE_M2:
        msg = f"blanket leaves {uncovered:.3f} m² of input geometry uncovered"
        raise SystemExit(msg)

    gdf = gpd.GeoDataFrame(
        [{**ATTRIBUTES, "geometry": blanket}], crs="EPSG:27700", geometry="geometry"
    )
    # A GeoPackage is SQLite: rewriting the layer in place leaves the old pages
    # allocated, so the file would keep the size of the layer it replaced.
    OUT.unlink(missing_ok=True)
    gdf.to_file(OUT, layer=LAYER, driver="GPKG")
    print(f"Wrote 1 feature ({blanket.area / 1e4:.0f} ha) to {OUT}")


if __name__ == "__main__":
    main()
