"""_prepare_coefficient_batch maps the England v2 coefficient schema."""

import geopandas as gpd
import pytest
from load_data import SpatialDataLoader
from shapely.geometry import MultiPolygon, Polygon


def _v2_gdf(**extra) -> gpd.GeoDataFrame:
    """Columns as published in NMSCoefficientLayer_England_v2_IAT.gpkg."""
    data = {
        "cromeid": ["RPA123"],
        "N_ResiCoeff": ["14.3"],
        "P_ResiCoeff": ["0.68"],
        "LU_CurrNcoeff": [20.1],
        "LU_CurrPcoeff": [0.3],
        "geometry": [MultiPolygon([Polygon([(0, 0), (0, 1), (1, 1), (1, 0)])])],
        **extra,
    }
    return gpd.GeoDataFrame(data, crs="EPSG:27700")


def _loader() -> SpatialDataLoader:
    # _prepare_coefficient_batch only reads class-level column config.
    return SpatialDataLoader.__new__(SpatialDataLoader)


def test_v2_schema_maps_to_model_columns():
    gdf = _loader()._prepare_coefficient_batch(_v2_gdf(), {})

    assert set(gdf.columns) == {
        "id",
        "version",
        "geometry",
        "crome_id",
        "lu_curr_n_coeff",
        "lu_curr_p_coeff",
        "n_resi_coeff",
        "p_resi_coeff",
    }
    row = gdf.iloc[0]
    assert row["crome_id"] == "RPA123"
    assert row["n_resi_coeff"] == pytest.approx(14.3)
    assert row["p_resi_coeff"] == pytest.approx(0.68)
    assert row["lu_curr_n_coeff"] == pytest.approx(20.1)
    assert row["lu_curr_p_coeff"] == pytest.approx(0.3)


def test_extra_source_columns_are_dropped():
    gdf = _v2_gdf(Land_use_cat=["ARABLE"], NN_Catchment=["THE BROADS SAC"])

    out = _loader()._prepare_coefficient_batch(gdf, {})

    assert "land_use_cat" not in out.columns
    assert "Land_use_cat" not in out.columns
    assert "NN_Catchment" not in out.columns


def test_missing_coefficient_column_still_rejected():
    gdf = _v2_gdf().drop(columns=["N_ResiCoeff"])
    loader = _loader()

    with pytest.raises(ValueError, match="n_resi_coeff"):
        loader._prepare_coefficient_batch(gdf, {})
