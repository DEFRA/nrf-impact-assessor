"""Settings for development scripts (data loading and testing).

All file paths are configured via environment variables in .env file.
No defaults are provided - everything must be explicitly configured in .env.
"""

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Get the directory where this settings.py file is located
_SCRIPT_DIR = Path(__file__).parent


class ScriptSettings(BaseSettings):
    """File path configuration for development scripts.

    All paths are loaded from scripts/.env file with no prefix.
    No defaults - must be configured in scripts/.env file.
    """

    model_config = SettingsConfigDict(
        env_file=str(_SCRIPT_DIR / ".env.local"),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Base directory for reference data
    base_path: Path = Field(description="Base directory containing reference data")

    # Spatial data files (relative to base_path)
    # Nutrient mitigation layers
    coefficient_gpkg: str = Field(description="Coefficient layer GeoPackage filename")
    coefficient_layer: str = Field(
        description="Layer name within coefficient GeoPackage"
    )
    wwtw_shapefile: str = Field(description="WwTW catchments shapefile path (relative)")
    lpa_shapefile: str = Field(description="LPA boundaries shapefile path (relative)")
    nn_catchment_shapefile: str = Field(
        description="NN catchments shapefile path (relative)"
    )
    subcatchment_shapefile: str = Field(
        description="Subcatchments shapefile path (relative)"
    )

    # GCN assessment layers
    gcn_risk_zones_gdb: str = Field(description="GCN Risk Zones GDB path (relative)")
    gcn_risk_zones_layer: str = Field(description="Layer name for GCN Risk Zones")
    gcn_ponds_gdb: str = Field(description="GCN Ponds GDB path (relative)")
    gcn_ponds_layer: str = Field(description="Layer name for GCN Ponds (National)")
    edp_edges_gdb: str = Field(description="EDP Edges GDB path (relative)")
    edp_edges_layer: str = Field(description="Layer name for EDP Edges")
    edp_boundary_gpkg: str = Field(description="EDP boundary GeoPackage path (relative)")
    edp_boundary_layer: str = Field(description="Layer name within EDP boundary GeoPackage")

    # Lookup database (relative to base_path)
    lookup_database: str = Field(description="SQLite lookup database path (relative)")

    # Test data (absolute or relative to project root)
    test_shapefile: str = Field(description="Default test shapefile for dev_test.py")

    # Output directory (absolute or relative to project root)
    output_dir: Path = Field(description="Output directory for results")

    @property
    def coefficient_gpkg_path(self) -> Path:
        """Full path to coefficient GeoPackage."""
        return self.base_path / self.coefficient_gpkg

    @property
    def wwtw_shapefile_path(self) -> Path:
        """Full path to WwTW shapefile."""
        return self.base_path / self.wwtw_shapefile

    @property
    def lpa_shapefile_path(self) -> Path:
        """Full path to LPA shapefile."""
        return self.base_path / self.lpa_shapefile

    @property
    def nn_catchment_shapefile_path(self) -> Path:
        """Full path to NN catchment shapefile."""
        return self.base_path / self.nn_catchment_shapefile

    @property
    def subcatchment_shapefile_path(self) -> Path:
        """Full path to subcatchment shapefile."""
        return self.base_path / self.subcatchment_shapefile

    @property
    def lookup_database_path(self) -> Path:
        """Full path to lookup database."""
        return self.base_path / self.lookup_database

    @property
    def test_shapefile_path(self) -> Path:
        """Full path to test shapefile."""
        return Path(self.test_shapefile)

    @property
    def gcn_risk_zones_gdb_path(self) -> Path:
        """Full path to GCN Risk Zones GDB."""
        return self.base_path / self.gcn_risk_zones_gdb

    @property
    def gcn_ponds_gdb_path(self) -> Path:
        """Full path to GCN Ponds GDB."""
        return self.base_path / self.gcn_ponds_gdb

    @property
    def edp_edges_gdb_path(self) -> Path:
        """Full path to EDP Edges GDB."""
        return self.base_path / self.edp_edges_gdb

    @property
    def edp_boundary_gpkg_path(self) -> Path:
        """Full path to EDP boundary GeoPackage."""
        return self.base_path / self.edp_boundary_gpkg
