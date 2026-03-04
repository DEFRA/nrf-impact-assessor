"""Sustainable Drainage Systems (SuDS) mitigation calculations.

Applies SuDS nutrient removal at the coefficient level, affecting only the
residential component (greenspace passes through unreduced).
"""

import numpy as np

from app.config import SuDsConfig


def apply_suds_mitigation(
    area_hectares,
    dev_area_ha,
    current_nitrogen_coeff,
    current_phosphorus_coeff,
    resi_n_component,
    gs_n_component,
    resi_p_component,
    gs_p_component,
    suds_config: SuDsConfig,
):
    """Apply SuDS mitigation at the coefficient level, residential component only.

    Args:
        area_hectares: Intersection area within NN catchment (hectares)
        dev_area_ha: Total development area (hectares), used for threshold check
        current_nitrogen_coeff: Current land use N coefficient (kg/ha/year)
        current_phosphorus_coeff: Current land use P coefficient (kg/ha/year)
        resi_n_component: Residential N coefficient component (from land_use calc)
        gs_n_component: Greenspace N coefficient component (from land_use calc)
        resi_p_component: Residential P coefficient component (from land_use calc)
        gs_p_component: Greenspace P coefficient component (from land_use calc)
        suds_config: SuDS configuration

    Returns:
        Tuple of (nitrogen_post_suds, phosphorus_post_suds) uplift in kg/year.
    """
    above_threshold = dev_area_ha >= suds_config.threshold_area_ha
    total_reduction = suds_config.total_reduction_factor

    adj_n_coeff = resi_n_component + gs_n_component
    adj_p_coeff = resi_p_component + gs_p_component

    suds_adj_n = np.where(
        above_threshold,
        (resi_n_component * (1 - total_reduction)) + gs_n_component,
        adj_n_coeff,
    )
    suds_adj_p = np.where(
        above_threshold,
        (resi_p_component * (1 - total_reduction)) + gs_p_component,
        adj_p_coeff,
    )

    nitrogen_post_suds = np.round(
        (suds_adj_n - current_nitrogen_coeff) * area_hectares, 2
    )
    phosphorus_post_suds = np.round(
        (suds_adj_p - current_phosphorus_coeff) * area_hectares, 2
    )

    return nitrogen_post_suds, phosphorus_post_suds
