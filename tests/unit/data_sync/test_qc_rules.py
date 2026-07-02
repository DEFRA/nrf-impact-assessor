from pathlib import Path

import pytest

from app.data_sync.qc_rules import load_qc_rules

_YAML_PATH = Path(__file__).parent.parent.parent.parent / "app/data_sync/qc_rules.yaml"


def test_load_qc_rules_reads_checked_in_yaml():
    rules = load_qc_rules(_YAML_PATH)
    assert rules.row_count_floor_pct == 90
    assert set(rules.tables) == {
        "coefficient_layer",
        "lookup_table",
        "wwtw_catchments",
        "lpa_boundaries",
        "nn_catchments",
        "subcatchments",
        "gcn_risk_zones",
        "gcn_ponds",
        "edp_edges",
        "edp_boundary_layer",
    }


def test_coefficient_layer_rules_have_column_key_and_ranges():
    rules = load_qc_rules(_YAML_PATH)
    cl = rules.tables["coefficient_layer"]
    assert cl.key is not None
    assert cl.key.source == "column"
    assert cl.key.columns == ["crome_id"]
    assert cl.key.unique is True
    assert cl.non_null_columns == ["land_use_cat", "nn_catchment", "subcatchment"]
    assert cl.coefficient_ranges["lu_curr_n_coeff"].min == 0
    assert cl.coefficient_ranges["lu_curr_n_coeff"].max == 50
    assert cl.coefficient_ranges["lu_curr_p_coeff"].max == 5
    assert cl.coefficient_ranges["n_resi_coeff"].max == 50
    assert cl.coefficient_ranges["p_resi_coeff"].max == 5
    assert cl.geometry is None  # coefficient_layer is exempt from rules 4-6


def test_lookup_table_rules_have_composite_key_and_lookup_rows():
    rules = load_qc_rules(_YAML_PATH)
    lt = rules.tables["lookup_table"]
    assert lt.key.source == "column"
    assert lt.key.columns == ["name", "version"]
    assert lt.lookup_rows["wwtw_lookup"].json_key == "wwtw_code"
    assert lt.lookup_rows["rates_lookup"].json_key == "nn_catchment"


def test_spatial_table_rules_have_json_key_and_geometry():
    rules = load_qc_rules(_YAML_PATH)
    nn = rules.tables["nn_catchments"]
    assert nn.key.source == "json"
    assert nn.key.columns == ["attributes.OID"]
    assert nn.key.unique is True
    assert nn.non_null_json_columns == ["attributes.N2K_Site_N"]
    assert nn.geometry.expected_type == "Polygon"
    assert nn.geometry.expected_srid == 27700

    lpa = rules.tables["lpa_boundaries"]
    assert lpa.geometry.expected_type == "MultiPolygon"

    gcn_ponds = rules.tables["gcn_ponds"]
    assert gcn_ponds.key is None  # no reliable business key (DM-2)
    assert gcn_ponds.geometry.expected_type == "MultiPolygon"


def test_gcn_risk_zones_has_allowed_values():
    rules = load_qc_rules(_YAML_PATH)
    rz = rules.tables["gcn_risk_zones"]
    assert rz.allowed_values["attributes.RZ"] == ["Red", "Amber", "Green"]


def test_referential_checks_loaded():
    rules = load_qc_rules(_YAML_PATH)
    names = {c.name for c in rules.referential_checks}
    assert names == {
        "rates_lookup_nn_catchment",
        "coefficient_layer_nn_catchment",
        "coefficient_layer_subcatchment",
        "wwtw_lookup_wwtw_code",
        "wwtw_lookup_subcatchment",
    }
    wwtw_code_check = next(
        c for c in rules.referential_checks if c.name == "wwtw_lookup_wwtw_code"
    )
    assert wwtw_code_check.numeric_coercion is True
    subcatchment_check = next(
        c for c in rules.referential_checks if c.name == "wwtw_lookup_subcatchment"
    )
    assert subcatchment_check.allow_null_from is True


def test_load_qc_rules_missing_file_raises():
    missing = Path("/nonexistent/qc_rules.yaml")
    with pytest.raises(FileNotFoundError):
        load_qc_rules(missing)
