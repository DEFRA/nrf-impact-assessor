"""edp_excluded_areas is registered across sync/QC/diagnostics."""

from pathlib import Path

import yaml

from app.config import DataSyncConfig
from app.data_sync.service import REFERENCE_TABLES


def test_in_data_sync_tables_allow_list():
    assert "edp_excluded_areas" in DataSyncConfig().tables


def test_in_reference_tables():
    labels = {label for _model, label in REFERENCE_TABLES}
    assert "edp_excluded_areas" in labels


def test_in_qc_rules():
    rules = yaml.safe_load(Path("app/data_sync/qc_rules.yaml").read_text())
    assert "edp_excluded_areas" in rules["tables"]
