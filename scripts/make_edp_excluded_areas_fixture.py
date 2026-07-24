"""Generate a small edp_excluded_areas.gpkg test fixture (3 polygons).

Run once: uv run python scripts/make_edp_excluded_areas_fixture.py
Produces tests/data/fixtures/edp_excluded_areas.gpkg with the source schema
(site_name, designation_type, buff_dist_m, Shape_Length, Shape_Area).
Geometries are representative squares in EPSG:27700, not the real buffered
SSSI polygons — enough to exercise load + QC in tests. They are MultiPolygon
because the real layer is, and qc_rules.yaml checks the declared type.
"""

from pathlib import Path

import geopandas as gpd
from shapely.geometry import MultiPolygon, box

OUT = Path("tests/data/fixtures/edp_excluded_areas.gpkg")

rows = [
    {
        "site_name": "Yare Broads and Marshes SSSI",
        "designation_type": "SSSI",
        "buff_dist_m": 50,
        "geometry": MultiPolygon([box(600000, 300000, 601000, 301000)]),
    },
    {
        "site_name": "Bure Broads and Marshes SSSI",
        "designation_type": "SSSI",
        "buff_dist_m": 50,
        "geometry": MultiPolygon([box(610000, 310000, 611000, 311000)]),
    },
    {
        "site_name": "River Wensum SSSI",
        "designation_type": "SSSI",
        "buff_dist_m": 50,
        "geometry": MultiPolygon([box(620000, 320000, 621000, 321000)]),
    },
]

gdf = gpd.GeoDataFrame(rows, crs="EPSG:27700")
gdf["Shape_Length"] = gdf.geometry.length
gdf["Shape_Area"] = gdf.geometry.area

OUT.parent.mkdir(parents=True, exist_ok=True)
gdf.to_file(OUT, layer="edp_excluded_areas", driver="GPKG")
print(f"Wrote {len(gdf)} features to {OUT}")
