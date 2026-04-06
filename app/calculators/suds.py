"""Sustainable Drainage Systems (SuDS) mitigation calculations.

Applies SuDS nutrient removal as a post-aggregation step on total land-use uplift,
matching IATScript.py lines 318-336.
"""

import numpy as np

from app.config import SuDsConfig


def apply_suds_mitigation(
    n_lu_uplift,
    p_lu_uplift,
    dwellings,
    suds_config: SuDsConfig,
):
    """Apply SuDS mitigation on aggregated land-use uplift totals.

    For developments with dwellings >= threshold, a percentage of the absolute
    uplift is subtracted.  Using abs() means negative uplifts (improvements)
    are amplified rather than reduced.

    Args:
        n_lu_uplift: Nitrogen land-use uplift (kg/year), per-RLB totals.
        p_lu_uplift: Phosphorus land-use uplift (kg/year), per-RLB totals.
        dwellings: Number of dwellings per development.
        suds_config: SuDS configuration.

    Returns:
        Tuple of (n_post_suds, p_post_suds) uplift in kg/year.
    """
    above_threshold = dwellings >= suds_config.threshold_dwellings
    total_reduction = np.where(above_threshold, suds_config.total_reduction_factor, 0)

    n_post_suds = np.round(n_lu_uplift - abs(n_lu_uplift) * total_reduction, 2)
    p_post_suds = np.round(p_lu_uplift - abs(p_lu_uplift) * total_reduction, 2)

    return n_post_suds, p_post_suds
