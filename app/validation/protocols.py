"""Validation protocol definitions."""

from typing import Protocol

import geopandas as gpd

from app.validation.errors import ValidationError


class DevelopmentDataValidator(Protocol):
    """Protocol for development data validation strategies.

    Allows different sources of development data (shapefile columns, user input, API, etc.)
    """

    def required_fields(self) -> list[str]:
        """Return list of required development data fields."""
        ...

    def validate(self, data: dict | gpd.GeoDataFrame) -> list[ValidationError]:
        """Validate development data from any source.

        Args:
            data: Either a GeoDataFrame (shapefile mode) or dict (user input mode)

        Returns:
            List of validation errors (empty if valid)
        """
        ...
