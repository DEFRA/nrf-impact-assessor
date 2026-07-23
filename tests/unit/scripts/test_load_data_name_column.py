"""_build_base_clean_gdf honours an explicit name_column override."""

import geopandas as gpd
from load_data import SpatialDataLoader
from shapely.geometry import Polygon


def _sample_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "Site name": ["Yare Broads and Marshes SSSI"],
            "designation_type": ["SSSI"],
            "geometry": [Polygon([(0, 0), (0, 1), (1, 1), (1, 0)])],
        },
        crs="EPSG:27700",
    )


def test_name_column_override_populates_name():
    clean = SpatialDataLoader._build_base_clean_gdf(
        _sample_gdf(), name_column="Site name"
    )
    assert clean["name"].iloc[0] == "Yare Broads and Marshes SSSI"


def test_no_name_column_falls_back_to_none_when_unmatched():
    clean = SpatialDataLoader._build_base_clean_gdf(_sample_gdf())
    # "Site name" is not in the default candidate list -> name is None
    assert clean["name"].iloc[0] is None
