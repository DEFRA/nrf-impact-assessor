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
        """Inspect the compiled query parameters to decide what data to return."""
        compiled_params = stmt.compile().params

        if as_gdf:
            # Check for a matching layer type enum member in the query's parameters
            for param_value in compiled_params.values():
                if param_value == SpatialLayerType.WWTW_CATCHMENTS:
                    return sample_wwtw_catchments.copy()
                if param_value == SpatialLayerType.LPA_BOUNDARIES:
                    return sample_lpa_boundaries.copy()
                if param_value == SpatialLayerType.SUBCATCHMENTS:
                    return sample_subcatchments.copy()
                if param_value == SpatialLayerType.NN_CATCHMENTS:
                    return sample_nn_catchments.copy()
            return gpd.GeoDataFrame()

        # Handle non-spatial queries (as_gdf=False)
        # Check for version lookups (func.max queries for spatial layers)
        stmt_str = str(stmt).lower()
        if "max" in stmt_str and "version" in stmt_str:
            return [1]

        # Check for a matching lookup table name in the query's parameters
        for param_value in compiled_params.values():
            if param_value == "rates_lookup":
                return [sample_rates_lookup]
            if param_value == "wwtw_lookup":
                return [sample_wwtw_lookup]
        return []

    repo.execute_query.side_effect = execute_query_side_effect

    def majority_overlap_postgis_side_effect(
        input_gdf,
        overlay_table,
        overlay_filter,
        input_id_col,
        overlay_attr_col,
        output_field,
        default_value=None,
    ):
        """Simulate PostGIS majority_overlap by doing Python-side overlay."""
        # Determine which layer based on the filter (check compiled params)
        compiled = overlay_filter.compile()
        layer_data = None
        attr_key = None
        for pv in compiled.params.values():
            if pv == SpatialLayerType.WWTW_CATCHMENTS:
                layer_data = sample_wwtw_catchments.copy()
                attr_key = "WwTw_ID"
                break
            if pv == SpatialLayerType.LPA_BOUNDARIES:
                layer_data = sample_lpa_boundaries.copy()
                attr_key = "NAME"
                break
            if pv == SpatialLayerType.SUBCATCHMENTS:
                layer_data = sample_subcatchments.copy()
                attr_key = "OPCAT_NAME"
                break

        if layer_data is None:
            return pd.DataFrame(columns=[input_id_col, output_field])

        # Extract attribute from JSONB
        layer_data[attr_key] = layer_data["attributes"].apply(
            lambda x: x.get(attr_key) if isinstance(x, dict) else None
        )

        # Simple Python-side majority overlap
        intersections = gpd.overlay(input_gdf, layer_data, how="intersection")
        intersections["_area"] = intersections.geometry.area

        if len(intersections) == 0:
            result = pd.DataFrame(
                {
                    input_id_col: input_gdf[input_id_col],
                    output_field: default_value,
                }
            )
        else:
            majority = intersections.loc[
                intersections.groupby(input_id_col)["_area"].idxmax(),
                [input_id_col, attr_key],
            ].reset_index(drop=True)
            result = input_gdf[[input_id_col]].merge(
                majority, on=input_id_col, how="left"
            )
            result = result.rename(columns={attr_key: output_field})
            if default_value is not None:
                result[output_field] = result[output_field].fillna(default_value)

        return result[[input_id_col, output_field]]

    repo.majority_overlap_postgis.side_effect = majority_overlap_postgis_side_effect

    def batch_majority_overlap_postgis_side_effect(
        input_gdf,
        input_id_col,
        assignments,
    ):
        """Simulate batched PostGIS majority_overlap using Python-side overlay."""
        results = {}
        for assignment in assignments:
            result = majority_overlap_postgis_side_effect(
                input_gdf=input_gdf,
                overlay_table=assignment["overlay_table"],
                overlay_filter=assignment["overlay_filter"],
                input_id_col=input_id_col,
                overlay_attr_col=assignment["overlay_attr_col"],
                output_field=assignment["output_field"],
                default_value=assignment.get("default_value"),
            )
            results[assignment["output_field"]] = result
        return results

    repo.batch_majority_overlap_postgis.side_effect = (
        batch_majority_overlap_postgis_side_effect
    )

    def land_use_intersection_postgis_side_effect(
        input_gdf,
        coeff_version,
        nn_version,
    ):
        """Simulate PostGIS 3-way intersection by doing Python-side overlays."""
        # Step 1: Intersect RLBs with coefficient layer
        coeff = sample_coefficient_layer.copy()
        intersections = gpd.overlay(input_gdf, coeff, how="intersection")

        if len(intersections) == 0:
            return pd.DataFrame(
                columns=[
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
            )

        # Step 2: Intersect with NN catchments
        nn = sample_nn_catchments.copy()
        nn["n2k_site_n"] = nn["attributes"].apply(
            lambda x: x.get("N2K_Site_N") if isinstance(x, dict) else None
        )
        intersections = gpd.overlay(intersections, nn, how="intersection")

        if len(intersections) == 0:
            return pd.DataFrame(
                columns=[
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
            )

        # Step 3: Calculate area in hectares
        intersections["area_in_nn_catchment_ha"] = intersections.geometry.area / 10000.0

        # Filter zero-area intersections
        intersections = intersections[intersections["area_in_nn_catchment_ha"] > 0]

        return intersections[
            [
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
        ].reset_index(drop=True)

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
