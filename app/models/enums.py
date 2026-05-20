"""Database enums."""

from enum import Enum, StrEnum


class AssessmentType(Enum):
    """Types of impact assessments supported by the worker."""

    NUTRIENT = "nutrient"
    GCN = "gcn"


class EdpType(StrEnum):
    """EDP types sent to nrf-backend in the PATCH /quotes payload."""

    NUTRIENT = "NUTRIENT"
    GCN = "GCN"
