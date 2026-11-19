"""Database enums."""

from enum import Enum


class AssessmentType(Enum):
    """Types of impact assessments supported by the worker."""

    NUTRIENT = "nutrient"
    GCN = "gcn"
