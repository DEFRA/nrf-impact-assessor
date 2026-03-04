"""Land use change nutrient uplift calculations.

Calculates nutrient impacts from converting existing land use to residential development,
with greenspace coefficient adjustment for larger sites.
"""

import numpy as np

from app.config import GreenspaceConfig


def calculate_land_use_uplift(
    area_hectares,
    dev_area_ha,
    current_nitrogen_coeff,
    residential_nitrogen_coeff,
    current_phosphorus_coeff,
    residential_phosphorus_coeff,
    greenspace_config: GreenspaceConfig,
):
    """Calculate nutrient uplift from land use change with greenspace adjustment.

    For developments >= greenspace threshold, the residential coefficient is split
    into a residential component and a greenspace component before computing uplift.

    Args:
        area_hectares: Intersection area within NN catchment (hectares)
        dev_area_ha: Total development area (hectares), used for threshold check
        current_nitrogen_coeff: Current land use N coefficient (kg/ha/year)
        residential_nitrogen_coeff: Residential land use N coefficient (kg/ha/year)
        current_phosphorus_coeff: Current land use P coefficient (kg/ha/year)
        residential_phosphorus_coeff: Residential land use P coefficient (kg/ha/year)
        greenspace_config: Greenspace configuration

    Returns:
        Tuple of (n_uplift, p_uplift, resi_n_component, gs_n_component,
                  resi_p_component, gs_p_component).
    """
    gs_threshold = greenspace_config.threshold_area_ha
    gs_fraction = greenspace_config.greenspace_percent / 100
    gs_n = greenspace_config.nitrogen_coeff
    gs_p = greenspace_config.phosphorus_coeff

    above_threshold = dev_area_ha >= gs_threshold

    resi_n_component = np.where(
        above_threshold,
        residential_nitrogen_coeff * (1 - gs_fraction),
        residential_nitrogen_coeff,
    )
    gs_n_component = np.where(above_threshold, gs_fraction * gs_n, 0)

    resi_p_component = np.where(
        above_threshold,
        residential_phosphorus_coeff * (1 - gs_fraction),
        residential_phosphorus_coeff,
    )
    gs_p_component = np.where(above_threshold, gs_fraction * gs_p, 0)

    adj_n_coeff = resi_n_component + gs_n_component
    adj_p_coeff = resi_p_component + gs_p_component

    nitrogen_uplift = np.round(
        (adj_n_coeff - current_nitrogen_coeff) * area_hectares, 2
    )
    phosphorus_uplift = np.round(
        (adj_p_coeff - current_phosphorus_coeff) * area_hectares, 2
    )

    return (
        nitrogen_uplift,
        phosphorus_uplift,
        resi_n_component,
        gs_n_component,
        resi_p_component,
        gs_p_component,
    )
