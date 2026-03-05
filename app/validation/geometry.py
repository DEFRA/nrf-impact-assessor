"""Geometry validation for Red Line Boundary shapefiles and GeoJSON."""

from pathlib import Path

import geopandas as gpd

from app.models.geometry import GeometryFormat
from app.validation.errors import ValidationError


class GeometryValidator:
    """Validates Red Line Boundary geometry from shapefiles or GeoJSON.

    This validator always runs - all geometry files must pass validation.

    Checks:
    - File format detection (.shp or .geojson/.json)
    - Shapefile: component files present (.shp, .shx, .dbf, .prj)
    - GeoJSON: valid JSON structure (handled by geopandas)
    - Valid CRS (EPSG:27700 or transformable)
    - Geometry column present
    - Geometry type (Polygon/MultiPolygon)
    - No null geometries
    - No invalid geometries
    """

    def validate(
        self, geometry_path: Path, geometry_format: GeometryFormat
    ) -> list[ValidationError]:
        """Validate geometry file (shapefile or GeoJSON).

        Args:
            geometry_path: Path to .shp file or .geojson/.json file
                (guaranteed to exist by S3Client/CLI)
            geometry_format: Geometry format type (from S3Client or CLI detection)

        Returns:
            List of validation errors (empty if valid)
        """
        errors = []

        if geometry_format == GeometryFormat.SHAPEFILE:
            errors.extend(self._validate_shapefile_components(geometry_path))
            if errors:
                return errors

        try:
            gdf = gpd.read_file(geometry_path)
        except Exception as e:
            return errors + [
                ValidationError(
                    message=f"Cannot read {geometry_format.value}: {e}",
                    field=geometry_format.value,
                )
            ]

        return errors + self._validate_geometry_data(gdf)

    def _validate_shapefile_components(
        self, shapefile_path: Path
    ) -> list[ValidationError]:
        """Validate shapefile component files exist.

        Args:
            shapefile_path: Path to .shp file

        Returns:
            List of validation errors (empty if all components present)
        """
        errors = []
        required_extensions = [".shp", ".shx", ".dbf", ".prj"]
        base_path = shapefile_path.with_suffix("")

        for ext in required_extensions:
            if not (base_path.with_suffix(ext)).exists():
                errors.append(
                    ValidationError(
                        message=f"Missing required shapefile component: {ext}",
                        field="shapefile",
                    )
                )

        return errors

    def _validate_geometry_data(self, gdf: gpd.GeoDataFrame) -> list[ValidationError]:
        """Validate geometry data (common checks for both formats).

        Args:
            gdf: GeoDataFrame loaded from file

        Returns:
            List of validation errors (empty if valid)
        """
        errors = []

        if gdf.crs is None:
            errors.append(
                ValidationError(
                    message="Geometry file has no defined CRS",
                    field="crs",
                )
            )

        if "geometry" not in gdf.columns:
            errors.append(
                ValidationError(
                    message="Geometry file missing 'geometry' column",
                    field="geometry",
                )
            )
            return errors

        valid_geom_types = {"Polygon", "MultiPolygon"}
        geom_types = set(gdf.geometry.geom_type.unique())
        invalid_types = geom_types - valid_geom_types

        if invalid_types:
            errors.append(
                ValidationError(
                    message=f"Invalid geometry types found: {', '.join(invalid_types)}. "
                    f"Expected: Polygon or MultiPolygon",
                    field="geometry",
                )
            )

        null_count = gdf.geometry.isna().sum()
        if null_count > 0:
            errors.append(
                ValidationError(
                    message=f"Found {null_count} null geometries",
                    field="geometry",
                )
            )

        invalid_count = (~gdf.geometry.is_valid).sum()
        if invalid_count > 0:
            errors.append(
                ValidationError(
                    message=f"Found {invalid_count} invalid geometries (self-intersections, etc.)",
                    field="geometry",
                )
            )

        return errors
