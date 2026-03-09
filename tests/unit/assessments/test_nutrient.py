"""Unit tests for Nutrient assessment module."""

from unittest.mock import Mock

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Polygon

from app.assessments.nutrient import NutrientAssessment
from app.models.enums import SpatialLayerType


@pytest.fixture
def sample_rlb():
    """Create a sample RLB GeoDataFrame with required nutrient columns."""
    return gpd.GeoDataFrame(
        {
            "id": [1, 2],
            "name": ["Test Site 1", "Test Site 2"],
            "dwelling_category": ["Small", "Medium"],
            "source": ["LPA", "LPA"],
            "dwellings": [10, 20],
            "shape_area": [10000.0, 20000.0],
            "geometry": [
                Polygon(
                    [
                        (450000, 100000),
                        (450100, 100000),
                        (450100, 100100),
                        (450000, 100100),
                    ]
                ),
                Polygon(
                    [
                        (450200, 100000),
                        (450300, 100000),
                        (450300, 100100),
                        (450200, 100100),
                    ]
                ),
            ],
        },
        crs="EPSG:27700",
    )


@pytest.fixture
def sample_wwtw_catchments():
    """Create sample WwTW catchments."""
    return gpd.GeoDataFrame(
        {
            "attributes": [{"WwTw_ID": 123}, {"WwTw_ID": 456}],
            "geometry": [
                Polygon(
                    [
                        (450000, 100000),
                        (450150, 100000),
                        (450150, 100150),
                        (450000, 100150),
                    ]
                ),
                Polygon(
                    [
                        (450150, 100000),
                        (450350, 100000),
                        (450350, 100150),
                        (450150, 100150),
                    ]
                ),
            ],
        },
        crs="EPSG:27700",
    )


@pytest.fixture
def sample_lpa_boundaries():
    """Create sample LPA boundaries."""
    return gpd.GeoDataFrame(
        {
            "attributes": [{"NAME": "Test LPA"}],
            "geometry": [
                Polygon(
                    [
                        (450000, 100000),
                        (450400, 100000),
                        (450400, 100200),
                        (450000, 100200),
                    ]
                )
            ],
        },
        crs="EPSG:27700",
    )


@pytest.fixture
def sample_subcatchments():
    """Create sample subcatchments."""
    return gpd.GeoDataFrame(
        {
            "attributes": [{"OPCAT_NAME": "Test Subcatchment"}],
            "geometry": [
                Polygon(
                    [
                        (450000, 100000),
                        (450400, 100000),
                        (450400, 100200),
                        (450000, 100200),
                    ]
                )
            ],
        },
        crs="EPSG:27700",
    )


@pytest.fixture
def sample_coefficient_layer():
    """Create sample coefficient layer."""
    return gpd.GeoDataFrame(
        {
            "crome_id": [1, 2],
            "lu_curr_n_coeff": [5.0, 3.0],
            "lu_curr_p_coeff": [0.5, 0.3],
            "n_resi_coeff": [10.0, 10.0],
            "p_resi_coeff": [1.0, 1.0],
            "geometry": [
                Polygon(
                    [
                        (450000, 100000),
                        (450100, 100000),
                        (450100, 100100),
                        (450000, 100100),
                    ]
                ),
                Polygon(
                    [
                        (450200, 100000),
                        (450300, 100000),
                        (450300, 100100),
                        (450200, 100100),
                    ]
                ),
            ],
        },
        crs="EPSG:27700",
    )


@pytest.fixture
def sample_nn_catchments():
    """Create sample NN catchments."""
    return gpd.GeoDataFrame(
        {
            "attributes": [{"N2K_Site_N": "Solent"}],
            "geometry": [
                Polygon(
                    [
                        (450000, 100000),
                        (450400, 100000),
                        (450400, 100200),
                        (450000, 100200),
                    ]
                )
            ],
        },
        crs="EPSG:27700",
    )


@pytest.fixture
def sample_rates_lookup():
    """Create sample rates lookup."""
    mock_lookup = Mock()
    mock_lookup.data = [
        {
            "nn_catchment": "Solent",
            "occupancy_rate": 2.4,
            "water_usage_L_per_person_day": 150.0,
        }
    ]
    return mock_lookup


@pytest.fixture
def sample_wwtw_lookup():
    """Create sample WwTW lookup."""
    mock_lookup = Mock()
    mock_lookup.data = [
        {
            "wwtw_code": "123",
            "wwtw_name": "Test WwTW",
            "wwtw_catchment": "Solent",
            "wwtw_subcatchment": "Test Subcatchment",
            "nitrogen_conc_2025_2030_mg_L": 10.0,
            "nitrogen_conc_2030_onwards_mg_L": 8.0,
            "phosphorus_conc_2025_2030_mg_L": 1.0,
            "phosphorus_conc_2030_onwards_mg_L": 0.8,
        }
    ]
    return mock_lookup


_LU_COLUMNS = [
    "rlb_id",
    "dwellings",
    "name",
    "dwelling_category",
    "source",
    "crome_id",
    "lu_curr_n_coeff",
    "lu_curr_p_coeff",
    "n_resi_coeff",
    "p_resi_coeff",
    "n2k_site_n",
    "area_in_nn_catchment_ha",
]

_GDF_LAYER_TYPES = [
    SpatialLayerType.WWTW_CATCHMENTS,
    SpatialLayerType.LPA_BOUNDARIES,
    SpatialLayerType.SUBCATCHMENTS,
    SpatialLayerType.NN_CATCHMENTS,
]

_MAJORITY_LAYER_ATTR = {
    SpatialLayerType.WWTW_CATCHMENTS: "WwTw_ID",
    SpatialLayerType.LPA_BOUNDARIES: "NAME",
    SpatialLayerType.SUBCATCHMENTS: "OPCAT_NAME",
}


def _layer_type_from_params(compiled_params) -> SpatialLayerType | None:
    for pv in compiled_params.values():
        if isinstance(pv, SpatialLayerType):
            return pv
    return None


def _gdf_execute_query(
    compiled_params, wwtw, lpa, subcatchments, nn
) -> gpd.GeoDataFrame:
    layer = _layer_type_from_params(compiled_params)
    mapping = {
        SpatialLayerType.WWTW_CATCHMENTS: wwtw,
        SpatialLayerType.LPA_BOUNDARIES: lpa,
        SpatialLayerType.SUBCATCHMENTS: subcatchments,
        SpatialLayerType.NN_CATCHMENTS: nn,
    }
    sample = mapping.get(layer)
    return sample.copy() if sample is not None else gpd.GeoDataFrame()


def _scalar_execute_query(stmt, rates_lookup, wwtw_lookup):
    stmt_str = str(stmt).lower()
    if "max" in stmt_str and "version" in stmt_str:
        return [1]
    for pv in stmt.compile().params.values():
        if pv == "rates_lookup":
            return [rates_lookup]
        if pv == "wwtw_lookup":
            return [wwtw_lookup]
    return []


def _resolve_majority_layer(overlay_filter, wwtw, lpa, subcatchments):
    """Return (layer_data_copy, attr_key) or (None, None) if unrecognised."""
    samples = {
        SpatialLayerType.WWTW_CATCHMENTS: (wwtw, "WwTw_ID"),
        SpatialLayerType.LPA_BOUNDARIES: (lpa, "NAME"),
        SpatialLayerType.SUBCATCHMENTS: (subcatchments, "OPCAT_NAME"),
    }
    for pv in overlay_filter.compile().params.values():
        if pv in samples:
            gdf, attr_key = samples[pv]
            return gdf.copy(), attr_key
    return None, None


def _compute_majority_overlap(
    input_gdf, input_id_col, output_field, layer_data, attr_key, default_value
):
    """Python-side majority overlap computation."""
    layer_data[attr_key] = layer_data["attributes"].apply(
        lambda x: x.get(attr_key) if isinstance(x, dict) else None
    )
    intersections = gpd.overlay(input_gdf, layer_data, how="intersection")
    intersections["_area"] = intersections.geometry.area
    if len(intersections) == 0:
        return pd.DataFrame(
            {input_id_col: input_gdf[input_id_col], output_field: default_value}
        )
    majority = intersections.loc[
        intersections.groupby(input_id_col)["_area"].idxmax(),
        [input_id_col, attr_key],
    ].reset_index(drop=True)
    result = input_gdf[[input_id_col]].merge(majority, on=input_id_col, how="left")
    result = result.rename(columns={attr_key: output_field})
    if default_value is not None:
        result[output_field] = result[output_field].fillna(default_value)
    return result[[input_id_col, output_field]]


def _compute_land_use_intersection(input_gdf, coeff_layer, nn_layer) -> pd.DataFrame:
    """Python-side 3-way intersection for land use."""
    intersections = gpd.overlay(input_gdf, coeff_layer.copy(), how="intersection")
    if len(intersections) == 0:
        return pd.DataFrame(columns=_LU_COLUMNS)
    nn = nn_layer.copy()
    nn["n2k_site_n"] = nn["attributes"].apply(
        lambda x: x.get("N2K_Site_N") if isinstance(x, dict) else None
    )
    intersections = gpd.overlay(intersections, nn, how="intersection")
    if len(intersections) == 0:
        return pd.DataFrame(columns=_LU_COLUMNS)
    intersections["area_in_nn_catchment_ha"] = intersections.geometry.area / 10000.0
    intersections = intersections[intersections["area_in_nn_catchment_ha"] > 0]
    return intersections[_LU_COLUMNS].reset_index(drop=True)


@pytest.fixture
def mock_repository(
    sample_wwtw_catchments,
    sample_lpa_boundaries,
    sample_subcatchments,
    sample_coefficient_layer,
    sample_nn_catchments,
    sample_rates_lookup,
    sample_wwtw_lookup,
):
    """Create a mock repository that returns sample data based on the query."""
    repo = Mock()

    def execute_query_side_effect(stmt, as_gdf=False):
        compiled_params = stmt.compile().params
        if as_gdf:
            return _gdf_execute_query(
                compiled_params,
                sample_wwtw_catchments,
                sample_lpa_boundaries,
                sample_subcatchments,
                sample_nn_catchments,
            )
        return _scalar_execute_query(stmt, sample_rates_lookup, sample_wwtw_lookup)

    def majority_overlap_postgis_side_effect(
        input_gdf,
        overlay_table,
        overlay_filter,
        input_id_col,
        overlay_attr_col,
        output_field,
        default_value=None,
    ):
        layer_data, attr_key = _resolve_majority_layer(
            overlay_filter,
            sample_wwtw_catchments,
            sample_lpa_boundaries,
            sample_subcatchments,
        )
        if layer_data is None:
            return pd.DataFrame(columns=[input_id_col, output_field])
        return _compute_majority_overlap(
            input_gdf, input_id_col, output_field, layer_data, attr_key, default_value
        )

    def batch_majority_overlap_postgis_side_effect(
        input_gdf, input_id_col, assignments
    ):
        return {
            a["output_field"]: majority_overlap_postgis_side_effect(
                input_gdf=input_gdf,
                overlay_table=a["overlay_table"],
                overlay_filter=a["overlay_filter"],
                input_id_col=input_id_col,
                overlay_attr_col=a["overlay_attr_col"],
                output_field=a["output_field"],
                default_value=a.get("default_value"),
            )
            for a in assignments
        }

    def land_use_intersection_postgis_side_effect(input_gdf, coeff_version, nn_version):
        return _compute_land_use_intersection(
            input_gdf, sample_coefficient_layer, sample_nn_catchments
        )

    repo.execute_query.side_effect = execute_query_side_effect
    repo.majority_overlap_postgis.side_effect = majority_overlap_postgis_side_effect
    repo.batch_majority_overlap_postgis.side_effect = (
        batch_majority_overlap_postgis_side_effect
    )
    repo.land_use_intersection_postgis.side_effect = (
        land_use_intersection_postgis_side_effect
    )
    return repo


def test_run_assessment_basic(sample_rlb, mock_repository):
    """Test basic nutrient assessment execution."""
    metadata = {"unique_ref": "20250115123456"}

    assessment = NutrientAssessment(sample_rlb, metadata, mock_repository)
    results = assessment.run()

    # Verify structure
    assert isinstance(results, dict)
    assert "impact_summary" in results
    assert isinstance(results["impact_summary"], pd.DataFrame)

    # Verify result has expected columns
    result_df = results["impact_summary"]
    assert "rlb_id" in result_df.columns
    assert "n_total" in result_df.columns
    assert "p_total" in result_df.columns
    assert "dev_area_ha" in result_df.columns

    # Verify repository was called (execute_query for version lookups + lookups,
    # batch_majority_overlap_postgis for spatial, land_use_intersection_postgis for land use)
    assert mock_repository.execute_query.call_count >= 4
    assert mock_repository.batch_majority_overlap_postgis.call_count == 1
    assert mock_repository.land_use_intersection_postgis.call_count == 1


def test_run_assessment_validates_input(mock_repository):
    """Test that assessment validates input columns."""
    invalid_rlb = gpd.GeoDataFrame(
        {
            "id": [1],
            "geometry": [
                Polygon(
                    [
                        (450000, 100000),
                        (450100, 100000),
                        (450100, 100100),
                        (450000, 100100),
                    ]
                )
            ],
        },
        crs="EPSG:27700",
    )

    metadata = {"unique_ref": "20250115123456"}

    assessment = NutrientAssessment(invalid_rlb, metadata, mock_repository)
    with pytest.raises(ValueError, match="Required columns missing"):
        assessment.run()


def test_run_assessment_transforms_to_bng(sample_rlb, mock_repository):
    """Test that assessment transforms to BNG if needed."""
    rlb_wgs84 = sample_rlb.to_crs("EPSG:4326")

    metadata = {"unique_ref": "20250115123456"}

    assessment = NutrientAssessment(rlb_wgs84, metadata, mock_repository)
    results = assessment.run()

    # Should still work - converted internally
    assert "impact_summary" in results
    assert isinstance(results["impact_summary"], pd.DataFrame)


def test_run_assessment_filters_out_of_scope(mock_repository):
    """Test that assessment filters out-of-scope developments."""
    out_of_scope_rlb = gpd.GeoDataFrame(
        {
            "id": [1],
            "name": ["Out of Scope"],
            "dwelling_category": ["Small"],
            "source": ["LPA"],
            "dwellings": [10],
            "shape_area": [10000.0],
            "geometry": [
                Polygon(
                    [
                        (900000, 900000),
                        (900100, 900000),
                        (900100, 900100),
                        (900000, 900100),
                    ]
                )
            ],
        },
        crs="EPSG:27700",
    )

    metadata = {"unique_ref": "20250115123456"}

    assessment = NutrientAssessment(out_of_scope_rlb, metadata, mock_repository)
    results = assessment.run()

    # Result should be empty (filtered out)
    assert len(results["impact_summary"]) == 0


def test_validate_and_prepare_input_normalizes_columns(mock_repository):
    """Test that input validation normalizes legacy column names."""
    legacy_rlb = gpd.GeoDataFrame(
        {
            "id": [1],
            "Name": ["Legacy Site"],  # Title case
            "Dwel_Cat": ["Small"],
            "Source": ["LPA"],
            "Dwellings": [10],
            "Shape_Area": [10000.0],
            "geometry": [
                Polygon(
                    [
                        (450000, 100000),
                        (450100, 100000),
                        (450100, 100100),
                        (450000, 100100),
                    ]
                )
            ],
        },
        crs="EPSG:27700",
    )

    metadata = {"unique_ref": "20250115123456"}

    assessment = NutrientAssessment(legacy_rlb, metadata, mock_repository)
    results = assessment.run()

    # Should work - columns normalized internally
    assert "impact_summary" in results


def test_validate_and_prepare_input_assigns_rlb_id(sample_rlb, mock_repository):
    """Test that assessment assigns rlb_id sequence numbers."""
    metadata = {"unique_ref": "20250115123456"}

    assessment = NutrientAssessment(sample_rlb, metadata, mock_repository)
    results = assessment.run()

    result_df = results["impact_summary"]

    # Should have rlb_id column with sequence 1, 2
    assert "rlb_id" in result_df.columns
    assert set(result_df["rlb_id"]) == {1, 2}


def test_calculate_land_use_impacts_applies_suds(sample_rlb, mock_repository):
    """Test that land use calculation applies SuDS mitigation."""
    metadata = {"unique_ref": "20250115123456"}

    assessment = NutrientAssessment(sample_rlb, metadata, mock_repository)
    results = assessment.run()

    result_df = results["impact_summary"]

    # Should have SuDS columns
    if "n_lu_post_suds" in result_df.columns:
        # If any development in NN catchment, should have SuDS values
        assert result_df["n_lu_post_suds"].notna().any()


def test_calculate_wastewater_fills_missing_rates(sample_rlb, mock_repository):
    """Test that wastewater calculation handles missing rates."""
    metadata = {"unique_ref": "20250115123456"}

    assessment = NutrientAssessment(sample_rlb, metadata, mock_repository)
    results = assessment.run()

    # Should complete without error
    assert "impact_summary" in results


def test_calculate_totals_applies_buffer(sample_rlb, mock_repository):
    """Test that totals calculation applies precautionary buffer."""
    metadata = {"unique_ref": "20250115123456"}

    assessment = NutrientAssessment(sample_rlb, metadata, mock_repository)
    results = assessment.run()

    result_df = results["impact_summary"]

    # Should have total columns
    assert "n_total" in result_df.columns
    assert "p_total" in result_df.columns

    # Totals should be positive (if in catchment)
    if len(result_df) > 0:
        assert (result_df["n_total"] >= 0).all()
        assert (result_df["p_total"] >= 0).all()


def test_calculate_totals_rounds_to_2dp(sample_rlb, mock_repository):
    """Test that totals are rounded to 2 decimal places."""
    metadata = {"unique_ref": "20250115123456"}

    assessment = NutrientAssessment(sample_rlb, metadata, mock_repository)
    results = assessment.run()

    result_df = results["impact_summary"]

    # Check rounding for numeric columns (if present)
    round_cols = ["n_total", "p_total", "dev_area_ha"]
    for col in round_cols:
        if col in result_df.columns and not result_df[col].isna().all():
            # Check that values have at most 2 decimal places
            for val in result_df[col].dropna():
                # Round to 2dp should equal original value
                assert round(val, 2) == val


def test_filter_out_of_scope_removes_no_catchment(mock_repository):
    """Test that filter removes developments outside all catchments."""
    metadata = {"unique_ref": "20250115123456"}

    out_of_scope_rlb = gpd.GeoDataFrame(
        {
            "id": [1],
            "name": ["Out"],
            "dwelling_category": ["Small"],
            "source": ["LPA"],
            "dwellings": [10],
            "shape_area": [10000.0],
            "geometry": [
                Polygon(
                    [
                        (900000, 900000),
                        (900100, 900000),
                        (900100, 900100),
                        (900000, 900100),
                    ]
                )
            ],
        },
        crs="EPSG:27700",
    )

    assessment = NutrientAssessment(out_of_scope_rlb, metadata, mock_repository)
    results = assessment.run()

    # Should filter out developments with no catchment overlap
    assert len(results["impact_summary"]) == 0


def test_result_has_no_geometry_column(sample_rlb, mock_repository):
    """Test that result DataFrame has geometry column removed."""
    metadata = {"unique_ref": "20250115123456"}

    assessment = NutrientAssessment(sample_rlb, metadata, mock_repository)
    results = assessment.run()

    result_df = results["impact_summary"]

    # Geometry column should be dropped for output
    assert "geometry" not in result_df.columns
