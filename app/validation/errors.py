"""Validation error definitions."""

from dataclasses import dataclass


@dataclass
class ValidationError:
    """Represents a validation error with descriptive message."""

    message: str
    field: str | None = None
