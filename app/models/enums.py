"""Database enums for PostgreSQL native ENUM types.

These enums are used for discriminator columns in unified models and
will be created as PostgreSQL ENUM types via Alembic migrations.
"""

from enum import Enum


class SpatialLayerType(Enum):
    """Types of supporting spatial reference data layers.

    Used as discriminator for SpatialLayer table to identify different
    types of spatial data (catchments, boundaries, etc.).

    Note: Coefficients have their own dedicated CoefficientLayer table
    for optimal query performance on the large 5.4M polygon dataset.
    """

    # Nutrient mitigation layers
    WWTW_CATCHMENTS = "wwtw_catchments"
    LPA_BOUNDARIES = "lpa_boundaries"
    NN_CATCHMENTS = "nn_catchments"
    SUBCATCHMENTS = "subcatchments"

    # GCN assessment layers
    GCN_RISK_ZONES = "gcn_risk_zones"  # GCN Red/Amber/Green zones
    GCN_PONDS = "gcn_ponds"  # National ponds dataset
    EDP_EDGES = "edp_edges"  # Environmental designation edges


class AssessmentType(Enum):
    """Types of impact assessments supported by the worker."""

    NUTRIENT = "nutrient"
    GCN = "gcn"
