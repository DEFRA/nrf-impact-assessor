"""edp_excluded_areas must require a site name.

The check-boundary exclusion gate keys off the returned site-name list, so a row
with no name would be dropped from that list and the boundary would wrongly
continue down the EDP route. The QC rule is what prevents an unnamed row from
ever becoming the active version.
"""

from app.data_sync.qc_rules import load_qc_rules


def test_edp_excluded_areas_requires_site_name():
    rules = load_qc_rules()

    assert rules.tables["edp_excluded_areas"].non_null_json_columns == [
        "attributes.site_name"
    ]
