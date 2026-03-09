"""Development data validator for files with embedded development attributes."""

import geopandas as gpd

from app.validation.errors import ValidationError


class EmbeddedDevelopmentDataValidator:
    """Validates development data embedded in geometry file properties/attributes.

    FOR CLI/LOCAL TESTING ONLY: This validator is used for CLI testing where
    development data is embedded directly in the geometry file (shapefile attributes
    or GeoJSON properties). This provides a simpler testing experience with a single
    input file.

    In production, geometry files contain ONLY geometry - all development data comes
    from the frontend form.

    Expects columns/properties: id, Name, Dwel_Cat, Source, Dwellings, Shape_Area
    """

    def required_fields(self) -> list[str]:
        """Return required fields for embedded development data.

        These fields should be present as shapefile attributes or GeoJSON properties.
        """
        return ["id", "Name", "Dwel_Cat", "Source", "Dwellings", "Shape_Area"]

    def validate(self, data: gpd.GeoDataFrame) -> list[ValidationError]:
        """Validate required development data fields are present.

        Args:
            data: GeoDataFrame read from geometry file (shapefile or GeoJSON)

        Returns:
            List of validation errors (empty if valid)
        """
        errors = []

        missing_cols = [
            col for col in self.required_fields() if col not in data.columns
        ]

        if missing_cols:
            errors.append(
                ValidationError(
                    message=f"Missing required columns: {', '.join(missing_cols)}",
                    field="columns",
                )
            )
            return errors

        try:
            numeric_vals = data["Dwellings"].apply(lambda x: isinstance(x, int | float))
            if not numeric_vals.all():
                non_numeric_count = (~numeric_vals).sum()
                errors.append(
                    ValidationError(
                        message=(
                            f"Found {non_numeric_count} non-numeric values in 'Dwellings' column"
                        ),
                        field="Dwellings",
                    )
                )
        except Exception as e:
            errors.append(
                ValidationError(
                    message=f"Error validating Dwellings column: {e}",
                    field="Dwellings",
                )
            )

        return errors
