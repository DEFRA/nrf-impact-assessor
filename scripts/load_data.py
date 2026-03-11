#!/usr/bin/env python

"""Load reference data into PostGIS.

This script loads spatial layers and lookup tables from file-based sources
into the PostGIS database for local development and integration testing.

IMPORTANT: This is a tactical solution for the PostGIS migration. A productionized
version of data loading with proper versioning, rollback capabilities, and
incremental updates is required but is not part of this refactoring work.

WARNING: This script performs DESTRUCTIVE operations. It will DELETE all existing
data for each layer type before inserting new data. Always backup important data
before running this script.

Configuration:
    File paths are configured via scripts/.env file.
    Copy scripts/.env.example to scripts/.env and customize as needed.

Usage:
    # Load all data
    uv run python scripts/load_data.py

    # Load specific layers only
    uv run python scripts/load_data.py --layer wwtw_catchments --layer lpa_boundaries

    # Load sample data only (smaller subset for testing)
    uv run python scripts/load_data.py --sample
"""

import json
import sqlite3
from pathlib import Path
from typing import Annotated, Any
from uuid import uuid4

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
import typer
from settings import ScriptSettings
from sqlalchemy import delete, func, select

from app.config import DatabaseSettings
from app.models.db import CoefficientLayer, EdpBoundaryLayer, LookupTable, SpatialLayer
from app.models.enums import SpatialLayerType
from app.repositories.engine import create_db_engine
from app.repositories.repository import Repository

CRS_BRITISH_NATIONAL_GRID = "EPSG:27700"

app = typer.Typer(help="Load spatial data into PostGIS database")


def clean_nan_values(obj: Any) -> Any:
    """Recursively clean NaN and inf values from nested data structures.

    Converts NaN, inf, and -inf to None for proper JSON serialization.
    Handles nested dicts, lists, and scalar values.

    Args:
        obj: Object to clean (dict, list, or scalar)

    Returns:
        Cleaned object with NaN/inf replaced by None
    """
    if isinstance(obj, dict):
        return {k: clean_nan_values(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_nan_values(item) for item in obj]
    if isinstance(obj, float):
        # Check for NaN or inf using numpy or math
        if np.isnan(obj) or np.isinf(obj):
            return None
        return obj
    if isinstance(obj, np.floating | np.integer):
        # Handle numpy scalar types
        if np.isnan(obj) or np.isinf(obj):
            return None
        return obj.item()  # Convert to Python native type
    if isinstance(obj, pd.Timestamp):
        # Handle pandas Timestamp objects
        return obj.isoformat()
    if pd.isna(obj):
        # Catch any remaining pandas NA types
        return None
    return obj


_NAME_COLUMN_CANDIDATES = ("name", "Name", "NAME", "label", "id")


def _find_name_column(gdf: gpd.GeoDataFrame) -> str | None:
    """Return the first recognised name column present in gdf, or None."""
    for candidate in _NAME_COLUMN_CANDIDATES:
        if candidate in gdf.columns:
            return candidate
    return None


class SpatialDataLoader:
    """Loads spatial data from files into PostGIS."""

    def __init__(
        self,
        repository: Repository,
        settings: ScriptSettings,
        sample_mode: bool = False,
    ):
        """Initialize loader.

        Args:
            repository: PostGIS repository for database access
            settings: Script settings with file paths from .env
            sample_mode: If True, load only small sample of data for testing
        """
        self.repository = repository
        self.settings = settings
        self.sample_mode = sample_mode
        self.sample_limit = 100 if sample_mode else None

        # File paths from settings
        # Nutrient mitigation layers
        self.coefficient_gpkg = settings.coefficient_gpkg_path
        self.coefficient_layer = settings.coefficient_layer
        self.wwtw_shapefile = settings.wwtw_shapefile_path
        self.lpa_shapefile = settings.lpa_shapefile_path
        self.nn_catchment_shapefile = settings.nn_catchment_shapefile_path
        self.subcatchment_shapefile = settings.subcatchment_shapefile_path
        self.lookup_database = settings.lookup_database_path

        # GCN assessment layers
        self.gcn_risk_zones_gdb = settings.gcn_risk_zones_gdb_path
        self.gcn_risk_zones_layer = settings.gcn_risk_zones_layer
        self.gcn_ponds_gdb = settings.gcn_ponds_gdb_path
        self.gcn_ponds_layer = settings.gcn_ponds_layer
        self.edp_edges_gdb = settings.edp_edges_gdb_path
        self.edp_edges_layer = settings.edp_edges_layer
        self.edp_boundary_gpkg = settings.edp_boundary_gpkg_path
        self.edp_boundary_layer = settings.edp_boundary_layer

    def load_all(self) -> None:
        """Load all reference data layers and lookups."""
        print("Loading all data...")
        self.load_spatial_layers()
        self.load_coefficient_layer()
        self.load_edp_boundaries()
        self.load_lookup_tables()
        print("All data loaded successfully!")

    def load_spatial_layers(self, layer_types: list[str] | None = None) -> None:
        """Load spatial layers (catchments, boundaries) from shapefiles.

        Args:
            layer_types: Optional list of layer type names to load.
                        If None, loads all layers. Does NOT include 'coefficients'.
        """
        # Define layer configurations for SpatialLayer table
        # Nutrient mitigation layers (shapefiles)
        # GCN layers (GeoDatabase files)
        # Coefficient layer loaded separately
        layers_config = {
            # Nutrient mitigation layers
            "wwtw_catchments": {
                "type": SpatialLayerType.WWTW_CATCHMENTS,
                "path": self.wwtw_shapefile,
            },
            "lpa_boundaries": {
                "type": SpatialLayerType.LPA_BOUNDARIES,
                "path": self.lpa_shapefile,
            },
            "nn_catchments": {
                "type": SpatialLayerType.NN_CATCHMENTS,
                "path": self.nn_catchment_shapefile,
            },
            "subcatchments": {
                "type": SpatialLayerType.SUBCATCHMENTS,
                "path": self.subcatchment_shapefile,
            },
            # GCN assessment layers
            "gcn_risk_zones": {
                "type": SpatialLayerType.GCN_RISK_ZONES,
                "path": self.gcn_risk_zones_gdb,
                "layer": self.gcn_risk_zones_layer,
            },
            "gcn_ponds": {
                "type": SpatialLayerType.GCN_PONDS,
                "path": self.gcn_ponds_gdb,
                "layer": self.gcn_ponds_layer,
            },
            "edp_edges": {
                "type": SpatialLayerType.EDP_EDGES,
                "path": self.edp_edges_gdb,
                "layer": self.edp_edges_layer,
            },
        }

        # Filter by requested layer types
        if layer_types:
            layers_config = {k: v for k, v in layers_config.items() if k in layer_types}

        for layer_name, config in layers_config.items():
            self._load_spatial_layer(
                layer_name=layer_name,
                layer_type=config["type"],
                file_path=config["path"],
                layer=config.get("layer"),  # Optional layer name for GeoDatabase files
            )

    def load_coefficient_layer(self) -> None:
        """Load coefficient layer (5.4M polygons) using to_postgis() method."""
        if not self.coefficient_gpkg.exists():
            print(f"Skipping coefficients: File not found at {self.coefficient_gpkg}")
            return

        print(f"Loading coefficients from {self.coefficient_gpkg.name}...")

        # Read spatial data
        gdf = gpd.read_file(self.coefficient_gpkg, layer=self.coefficient_layer)

        # Ensure CRS is EPSG:27700
        if gdf.crs is None:
            print("No CRS found, assuming EPSG:27700")
            gdf = gdf.set_crs(CRS_BRITISH_NATIONAL_GRID)
        elif gdf.crs.to_epsg() != 27700:
            print(f"Reprojecting from {gdf.crs} to EPSG:27700")
            gdf = gdf.to_crs(CRS_BRITISH_NATIONAL_GRID)

        # Force 2D geometries (remove Z dimension if present)
        if gdf.geometry.has_z.any():
            print("Converting 3D geometries to 2D")
            gdf.geometry = gdf.geometry.apply(
                lambda geom: shapely.force_2d(geom) if geom else geom
            )

        # Sample mode: take first N records
        if self.sample_mode and len(gdf) > self.sample_limit:
            print(f"Sample mode: using {self.sample_limit} of {len(gdf)} features")
            gdf = gdf.head(self.sample_limit)

        total_features = len(gdf)
        print(f"Loaded {total_features} features")

        # Prepare DataFrame for CoefficientLayer model
        # Keep only columns that match the model fields
        expected_columns = [
            "crome_id",
            "land_use_cat",
            "nn_catchment",
            "subcatchment",
            "lu_curr_n_coeff",
            "lu_curr_p_coeff",
            "n_resi_coeff",
            "p_resi_coeff",
            "geometry",
        ]

        # Map source columns to model columns
        # Direct mapping for coefficient columns with known source names
        column_mapping = {
            "Land_use_cat": "land_use_cat",
            "NN_Catchment": "nn_catchment",
            "SubCatchment": "subcatchment",
            "LU_CurrNcoeff": "lu_curr_n_coeff",
            "LU_CurrPcoeff": "lu_curr_p_coeff",
            "N_ResiCoeff": "n_resi_coeff",
            "P_ResiCoeff": "p_resi_coeff",
            "cromeid": "crome_id",  # Normalize to snake_case
        }

        # Apply column mapping
        print(f"Mapping columns: {column_mapping}")
        gdf = gdf.rename(columns=column_mapping)

        # Keep only expected columns that exist
        available_columns = [col for col in expected_columns if col in gdf.columns]
        missing_columns = [
            col
            for col in expected_columns
            if col not in available_columns and col != "geometry"
        ]

        print(f"Available columns: {available_columns}")
        if missing_columns:
            error_msg = f"Missing expected columns: {missing_columns}"
            print(f"ERROR: {error_msg}")
            raise ValueError(error_msg)

        gdf = gdf[available_columns]

        # Clean coefficient columns - convert non-numeric values to None
        coeff_columns = [
            "lu_curr_n_coeff",
            "lu_curr_p_coeff",
            "n_resi_coeff",
            "p_resi_coeff",
        ]
        for col in coeff_columns:
            if col in gdf.columns:
                # Convert to numeric, coercing errors to NaN, then to None for SQL NULL
                gdf[col] = pd.to_numeric(gdf[col], errors="coerce")
                print(f"  {col}: {gdf[col].isna().sum()} null values after cleaning")

        # Add UUID and version columns
        gdf["id"] = [uuid4() for _ in range(len(gdf))]
        gdf["version"] = 1

        # Clear existing coefficient data
        with self.repository.session() as session:
            deleted = session.execute(delete(CoefficientLayer))
            session.commit()
            if deleted.rowcount > 0:
                print(f"Deleted {deleted.rowcount} existing coefficient records")

        # Load to PostGIS using fast to_postgis()
        print(f"Loading {total_features} coefficient features to PostGIS...")
        engine = self.repository.engine

        gdf.to_postgis(
            name="coefficient_layer",
            con=engine,
            schema="nrf_reference",
            if_exists="append",
            index=False,
            chunksize=10000,
        )

        print(f"Successfully loaded {total_features} coefficient records")

        # Verify insertion
        with self.repository.session() as session:
            count = session.scalar(select(func.count()).select_from(CoefficientLayer))
            print(f"Verified {count} coefficient records in database")

    def _load_spatial_layer(
        self,
        layer_name: str,
        layer_type: SpatialLayerType,
        file_path: Path,
        layer: str | None = None,
    ) -> None:
        """Load a single spatial layer into PostGIS using fast to_postgis() method.

        Args:
            layer_name: Human-readable layer name for logging
            layer_type: SpatialLayerType enum value
            file_path: Path to shapefile or geopackage
            layer: Layer name for geopackage (None for shapefile)
        """
        if not file_path.exists():
            print(f"Skipping {layer_name}: File not found at {file_path}")
            return

        print(f"Loading {layer_name} from {file_path.name}...")

        # Read spatial data
        gdf = (
            gpd.read_file(file_path, layer=layer) if layer else gpd.read_file(file_path)
        )

        # Ensure CRS is EPSG:27700
        if gdf.crs is None:
            print("No CRS found, assuming EPSG:27700")
            gdf = gdf.set_crs(CRS_BRITISH_NATIONAL_GRID)
        elif gdf.crs.to_epsg() != 27700:
            print(f"Reprojecting from {gdf.crs} to EPSG:27700")
            gdf = gdf.to_crs(CRS_BRITISH_NATIONAL_GRID)

        # Force 2D geometries (remove Z dimension if present)
        if gdf.geometry.has_z.any():
            print("Converting 3D geometries to 2D")
            gdf.geometry = gdf.geometry.apply(
                lambda geom: shapely.force_2d(geom) if geom else geom
            )

        # Sample mode: take first N records
        if self.sample_mode and len(gdf) > self.sample_limit:
            print(f"Sample mode: using {self.sample_limit} of {len(gdf)} features")
            gdf = gdf.head(self.sample_limit)

        total_features = len(gdf)
        print(f"Loaded {total_features} features")

        # Prepare DataFrame for SpatialLayer model
        name_col = _find_name_column(gdf)

        # Create clean DataFrame with required columns
        clean_gdf = gpd.GeoDataFrame(geometry=gdf.geometry, crs=gdf.crs)
        clean_gdf["id"] = [uuid4() for _ in range(len(clean_gdf))]
        clean_gdf["layer_type"] = layer_type.name
        clean_gdf["version"] = 1

        if name_col:
            clean_gdf["name"] = gdf[name_col].astype(str)
        else:
            clean_gdf["name"] = None

        # Store all non-geometry columns as JSONB attributes
        # This preserves all source attributes for flexible querying
        attribute_columns = [col for col in gdf.columns if col != "geometry"]
        if attribute_columns:
            # Use to_dict('records') instead of iterrows() for faster extraction
            records = gdf[attribute_columns].to_dict("records")
            clean_gdf["attributes"] = [
                json.dumps(clean_nan_values(rec)) for rec in records
            ]
        else:
            clean_gdf["attributes"] = None

        # Clear existing data for this layer type
        with self.repository.session() as session:
            deleted = session.execute(
                delete(SpatialLayer).where(SpatialLayer.layer_type == layer_type)
            )
            session.commit()
            if deleted.rowcount > 0:
                print(f"Deleted {deleted.rowcount} existing records")

        # Load to PostGIS using fast to_postgis()
        print(f"Loading {total_features} features to PostGIS...")
        engine = self.repository.engine

        clean_gdf.to_postgis(
            name="spatial_layer",
            con=engine,
            schema="nrf_reference",
            if_exists="append",
            index=False,
            chunksize=5000,
        )

        print(f"Successfully loaded {total_features} records")

        # Verify insertion
        with self.repository.session() as session:
            count = session.scalar(
                select(func.count())
                .select_from(SpatialLayer)
                .where(SpatialLayer.layer_type == layer_type)
            )
            print(f"Verified {count} records in database")

    def load_edp_boundaries(self) -> None:
        """Load EDP boundary polygons into dedicated edp_boundary_layer table."""
        if not self.edp_boundary_gpkg.exists():
            print(
                f"Skipping edp_boundaries: File not found at {self.edp_boundary_gpkg}"
            )
            return

        print(f"Loading edp_boundaries from {self.edp_boundary_gpkg.name}...")

        gdf = gpd.read_file(self.edp_boundary_gpkg, layer=self.edp_boundary_layer)

        if gdf.crs is None:
            print("No CRS found, assuming EPSG:27700")
            gdf = gdf.set_crs(CRS_BRITISH_NATIONAL_GRID)
        elif gdf.crs.to_epsg() != 27700:
            print(f"Reprojecting from {gdf.crs} to EPSG:27700")
            gdf = gdf.to_crs(CRS_BRITISH_NATIONAL_GRID)

        if gdf.geometry.has_z.any():
            print("Converting 3D geometries to 2D")
            gdf.geometry = gdf.geometry.apply(
                lambda geom: shapely.force_2d(geom) if geom else geom
            )

        if self.sample_mode and len(gdf) > self.sample_limit:
            print(f"Sample mode: using {self.sample_limit} of {len(gdf)} features")
            gdf = gdf.head(self.sample_limit)

        total_features = len(gdf)
        print(f"Loaded {total_features} features")

        name_col = _find_name_column(gdf)
        attribute_columns = [col for col in gdf.columns if col != "geometry"]

        clean_gdf = gpd.GeoDataFrame(geometry=gdf.geometry, crs=gdf.crs)
        clean_gdf["id"] = [uuid4() for _ in range(len(clean_gdf))]
        clean_gdf["version"] = 1
        clean_gdf["name"] = gdf[name_col].astype(str) if name_col else None

        if attribute_columns:
            records = gdf[attribute_columns].to_dict("records")
            clean_gdf["attributes"] = [
                json.dumps(clean_nan_values(rec)) for rec in records
            ]
        else:
            clean_gdf["attributes"] = None

        with self.repository.session() as session:
            deleted = session.execute(delete(EdpBoundaryLayer))
            session.commit()
            if deleted.rowcount > 0:
                print(f"Deleted {deleted.rowcount} existing records")

        print(f"Loading {total_features} features to PostGIS...")
        clean_gdf.to_postgis(
            name="edp_boundary_layer",
            con=self.repository.engine,
            schema="nrf_reference",
            if_exists="append",
            index=False,
            chunksize=5000,
        )

        print(f"Successfully loaded {total_features} records")

        with self.repository.session() as session:
            count = session.scalar(select(func.count()).select_from(EdpBoundaryLayer))
            print(f"Verified {count} records in database")

    def load_lookup_tables(self) -> None:
        """Load lookup tables from SQLite database."""
        if not self.lookup_database.exists():
            print(f"Skipping lookup tables: File not found at {self.lookup_database}")
            return

        print(f"Loading lookup tables from {self.lookup_database.name}...")

        # Connect to SQLite
        conn = sqlite3.connect(self.lookup_database)

        # Load WwTW lookup
        self._load_lookup_table(
            conn=conn,
            table_name="WwTw_lookup",
            name="wwtw_lookup",
            description="WwTW permits and characteristics",
        )

        # Load Rates lookup
        self._load_lookup_table(
            conn=conn,
            table_name="rates_lookup",
            name="rates_lookup",
            description="Nutrient generation rates by catchment",
        )

        conn.close()

    def _load_lookup_table(
        self, conn: sqlite3.Connection, table_name: str, name: str, description: str
    ) -> None:
        """Load a single lookup table from SQLite.

        Args:
            conn: SQLite database connection
            table_name: Table name in SQLite database
            name: Identifier for lookup in PostGIS
            description: Human-readable description
        """
        try:
            # Read table from SQLite
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
            print(f"Loaded {len(df)} rows from {table_name}")

            # Normalize legacy column names to snake_case
            column_name_map = {
                "NN_Catchment": "nn_catchment",
                "Occ_Rate": "occupancy_rate",
                "Water_Usage_L_Day": "water_usage_L_per_person_day",
                "Subcatchment_name": "wwtw_subcatchment",
                "WwTW_code": "wwtw_code",
                "WwTW_name": "wwtw_name",
                "Nitrogen_2025_2030": "nitrogen_conc_2025_2030_mg_L",
                "Nitrogen_2030_onwards": "nitrogen_conc_2030_onwards_mg_L",
                "Phosphorus_2025_2030": "phosphorus_conc_2025_2030_mg_L",
                "Phosphorus_2030_onwards": "phosphorus_conc_2030_onwards_mg_L",
            }
            # Only rename columns that are actually present
            columns_to_rename = {
                k: v for k, v in column_name_map.items() if k in df.columns
            }
            if columns_to_rename:
                df = df.rename(columns=columns_to_rename)
                print(f"Normalized {len(columns_to_rename)} column names to snake_case")

            # Replace NaN with None for proper JSON serialization
            df = df.where(pd.notna(df), None)

            # Convert DataFrame to list of dicts for JSONB storage
            data = df.to_dict(orient="records")

            # Deep clean any remaining NaN/inf values that survived to_dict()
            data = clean_nan_values(data)

            # Clear existing lookup
            with self.repository.session() as session:
                deleted = session.execute(
                    delete(LookupTable).where(LookupTable.name == name)
                )
                session.commit()
                if deleted.rowcount > 0:
                    print(f"Deleted {deleted.rowcount} existing versions")

            # Insert new lookup
            lookup_record = LookupTable(
                name=name,
                version=1,
                data=data,
                description=description,
            )

            with self.repository.session() as session:
                session.add(lookup_record)
                session.commit()

            print(f"Inserted lookup '{name}' with {len(data)} rows")

        except Exception as e:
            print(f"Error loading {table_name}: {e}")


def _validate_names(names: list[str] | None, valid: list[str], kind: str) -> None:
    """Exit with an error if any name is not in the valid set."""
    if names:
        invalid = [n for n in names if n not in valid]
        if invalid:
            typer.secho(
                f"Invalid {kind} names: {', '.join(invalid)}",
                fg=typer.colors.RED,
                err=True,
            )
            typer.secho(
                f"Valid choices: {', '.join(valid)}", fg=typer.colors.YELLOW, err=True
            )
            raise typer.Exit(code=1)


def _print_load_summary(
    settings: "ScriptSettings",
    layer: list[str] | None,
    lookup: list[str] | None,
    sample: bool,
) -> None:
    """Print a destructive-operation warning and summary of what will be loaded."""
    typer.secho(
        "\nWARNING: This operation is DESTRUCTIVE!", fg=typer.colors.RED, bold=True
    )
    typer.secho(
        "This will DELETE all existing data for the layers being loaded and replace it with new data.",
        fg=typer.colors.YELLOW,
    )
    typer.secho(f"\nData source: {settings.base_path}", fg=typer.colors.CYAN)
    typer.secho(
        f"Layers to load: {', '.join(layer)}" if layer else "Layers to load: ALL",
        fg=typer.colors.CYAN,
    )
    typer.secho(
        f"Lookups to load: {', '.join(lookup)}" if lookup else "Lookups to load: ALL",
        fg=typer.colors.CYAN,
    )
    if sample:
        typer.secho("Mode: SAMPLE (100 features per layer)", fg=typer.colors.CYAN)


@app.command()
def main(
    layer: Annotated[
        list[str] | None,
        typer.Option(
            help="Specific spatial layer(s) to load. Can be specified multiple times. "
            "Choices: wwtw_catchments, lpa_boundaries, nn_catchments, subcatchments, coefficients, "
            "gcn_risk_zones, gcn_ponds, edp_edges, edp_boundaries"
        ),
    ] = None,
    lookup: Annotated[
        list[str] | None,
        typer.Option(
            help="Specific lookup table(s) to load. Can be specified multiple times. "
            "Choices: wwtw_lookup, rates_lookup"
        ),
    ] = None,
    sample: Annotated[
        bool,
        typer.Option(help="Load only sample data for testing (100 features per layer)"),
    ] = False,
) -> None:
    """Load reference data into PostGIS database.

    File paths are configured via .env file. See scripts/.env.example for configuration.
    """
    # Validate layer and lookup names
    valid_layers = [
        "wwtw_catchments",
        "lpa_boundaries",
        "nn_catchments",
        "subcatchments",
        "coefficients",
        "gcn_risk_zones",
        "gcn_ponds",
        "edp_edges",
        "edp_boundaries",
    ]
    valid_lookups = ["wwtw_lookup", "rates_lookup"]
    _validate_names(layer, valid_layers, "layer")
    _validate_names(lookup, valid_lookups, "lookup")

    # Load settings from .env
    settings = ScriptSettings()

    # Create repository
    db_settings = DatabaseSettings()
    engine = create_db_engine(db_settings)
    repository = Repository(engine)

    # Create loader
    loader = SpatialDataLoader(repository, settings, sample_mode=sample)

    # Confirmation prompt with warning
    _print_load_summary(settings, layer, lookup, sample)
    typer.echo()
    confirm = typer.confirm("Do you want to continue?")

    if not confirm:
        typer.secho("Operation cancelled.", fg=typer.colors.YELLOW)
        raise typer.Exit(code=0)

    try:
        # Determine what to load
        load_all_layers = layer is None
        load_all_lookups = lookup is None

        if load_all_layers and load_all_lookups:
            # Load everything
            loader.load_all()
        else:
            # Load specific items
            if layer:
                # Separate out layers that have their own dedicated tables
                regular_layers = [
                    layer_name
                    for layer_name in layer
                    if layer_name not in ("coefficients", "edp_boundaries")
                ]
                load_coefficients = "coefficients" in layer
                load_edp_boundaries = "edp_boundaries" in layer

                if regular_layers:
                    loader.load_spatial_layers(layer_types=regular_layers)

                if load_coefficients:
                    loader.load_coefficient_layer()

                if load_edp_boundaries:
                    loader.load_edp_boundaries()

            if lookup:
                # Load specific lookups - need to update load_lookup_tables to accept filter
                typer.secho(
                    "Selective lookup loading not yet implemented, loading all lookups",
                    fg=typer.colors.YELLOW,
                )
                loader.load_lookup_tables()

    except Exception as e:
        typer.secho(f"Error: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from None

    finally:
        repository.close()


if __name__ == "__main__":
    app()
