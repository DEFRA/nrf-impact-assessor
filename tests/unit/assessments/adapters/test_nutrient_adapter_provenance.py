"""Provenance threading through the nutrient adapter (DM-3)."""

from uuid import uuid4

import pandas as pd

from app.assessments.adapters import nutrient_adapter
from app.models.domain import DataProvenance


def _impact_summary():
    return pd.DataFrame(
        [
            {
                "rlb_id": 1,
                "id": "site_001",
                "name": "Test",
                "dwelling_category": "Small",
                "source": "LPA",
                "dwellings": 10,
                "shape_area": 5000.0,
                "dev_area_ha": 0.5,
                "majority_wwtw_id": 123,
                "wwtw_name": None,
                "wwtw_subcatchment": None,
                "majority_name": "Test LPA",
                "nn_catchment": None,
                "nn_catchment_entries": None,
                "majority_opcat_name": None,
                "area_in_nn_catchment_ha": 0.3,
                "n_lu_uplift": 5.25,
                "p_lu_uplift": 0.45,
                "n_lu_post_suds": 4.72,
                "p_lu_post_suds": 0.41,
                "n_wwtw_perm": None,
                "p_wwtw_perm": None,
                "n_total": 10.0,
                "p_total": 1.0,
            }
        ]
    )


def test_provenance_attached_to_results():
    prov = DataProvenance(data_version="2026.07.01", data_sync_run_id=uuid4())
    out = nutrient_adapter.to_domain_models(
        {"impact_summary": _impact_summary()}, provenance=prov
    )
    assert out["assessment_results"][0].provenance == prov


def test_provenance_defaults_to_none():
    out = nutrient_adapter.to_domain_models({"impact_summary": _impact_summary()})
    assert out["assessment_results"][0].provenance is None
