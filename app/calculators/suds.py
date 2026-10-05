"""Sustainable Drainage Systems (SuDS) mitigation calculations.

Reduces the residential nutrient coefficient for developments at or above the
SuDS area threshold, matching IATScript.py lines 308-340. The greenspace
component is assumed not to be subject to SuDS treatment, so callers apply this
to the residential component only.
"""

import numpy as np

from app.config import SuDsConfig


def apply_suds_mitigation(residential_coeff, dev_area_ha, suds_config: SuDsConfig):
    """Apply SuDS removal to a residential nutrient coefficient.

    Args:
        residential_coeff: Residential component of the nutrient coefficient
            (kg/ha/year), after any greenspace split.
        dev_area_ha: Total development area (hectares), used for threshold check.
        suds_config: SuDS configuration.

    Returns:
        Residential coefficient after SuDS removal (kg/ha/year).
    """
    above_threshold = dev_area_ha >= suds_config.threshold_area_ha
    return np.where(
        above_threshold,
        residential_coeff * (1 - suds_config.total_reduction_factor),
        residential_coeff,
    )
