import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

from pydantic import Field, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppConfig(BaseSettings):
    model_config = SettingsConfigDict()
    python_env: str | None = None
    host: str = "127.0.0.1"
    port: int = 8086
    log_config: str | None = None
    mongo_uri: str | None = None
    mongo_database: str = "nrf-impact-assessor"
    mongo_truststore: str = "TRUSTSTORE_CDP_ROOT_CA"
    aws_endpoint_url: str | None = None
    http_proxy: HttpUrl | None = None
    enable_metrics: bool = False
    tracing_header: str = "x-cdp-request-id"


config = AppConfig()


# ---------------------------------------------------------------------------
# Physical constants (immutable)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PhysicalConstants:
    """Physical and mathematical constants used in impact calculations."""

    CRS_BRITISH_NATIONAL_GRID: str = "EPSG:27700"
    DAYS_PER_YEAR: float = 365.25
    SQUARE_METRES_PER_HECTARE: float = 10_000.0
    MILLIGRAMS_PER_KILOGRAM: float = 1_000_000.0


CONSTANTS = PhysicalConstants()


# ---------------------------------------------------------------------------
# Assessment configuration
# ---------------------------------------------------------------------------


class GreenspaceConfig(BaseSettings):
    """Configuration for greenspace adjustment in large developments."""

    model_config = SettingsConfigDict(
        env_prefix="GS_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    threshold_area_ha: float = Field(
        default=2.5,
        description="Development area (ha) above which greenspace is assumed",
    )
    greenspace_percent: float = Field(
        default=20.0, description="Percentage of development assumed as greenspace"
    )
    nitrogen_coeff: float = Field(
        default=3.0, description="Greenspace nitrogen coefficient (kg/ha/year)"
    )
    phosphorus_coeff: float = Field(
        default=0.2, description="Greenspace phosphorus coefficient (kg/ha/year)"
    )


class SuDsConfig(BaseSettings):
    """Configuration for Sustainable Drainage Systems (SuDS) mitigation."""

    model_config = SettingsConfigDict(
        env_prefix="SUDS_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    threshold_area_ha: float = Field(
        default=2.5, description="Development area (ha) above which SuDS is applied"
    )
    flow_capture_percent: float = Field(
        default=100.0, description="Percentage of flow entering SuDS system"
    )
    removal_rate_percent: float = Field(
        default=40.0, description="SuDS nutrient removal rate (%)"
    )

    @property
    def total_reduction_factor(self) -> float:
        return (self.flow_capture_percent / 100) * (self.removal_rate_percent / 100)


class AssessmentConfig(BaseSettings):
    """Main configuration for nutrient impact assessment business rules."""

    model_config = SettingsConfigDict(
        env_prefix="IAT_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    precautionary_buffer_percent: float = Field(
        default=20.0,
        description="Precautionary buffer added to total nutrient impacts (%)",
    )
    greenspace: GreenspaceConfig = Field(
        default_factory=GreenspaceConfig,
        description="Greenspace adjustment configuration",
    )
    suds: SuDsConfig = Field(
        default_factory=SuDsConfig, description="SuDS mitigation configuration"
    )
    fallback_wwtw_id: int = Field(
        default=141, description="WwTW ID for developments outside modeled catchments"
    )

    @property
    def precautionary_buffer_factor(self) -> float:
        return self.precautionary_buffer_percent / 100


class RequiredColumns:
    """Required column names in input Red Line Boundary shapefile (normalized snake_case)."""

    ID = "id"
    NAME = "name"
    DWELLING_CATEGORY = "dwelling_category"
    SOURCE = "source"
    DWELLINGS = "dwellings"
    SHAPE_AREA = "shape_area"
    GEOMETRY = "geometry"

    @classmethod
    def all(cls) -> list[str]:
        return [
            cls.ID,
            cls.NAME,
            cls.DWELLING_CATEGORY,
            cls.SOURCE,
            cls.DWELLINGS,
            cls.SHAPE_AREA,
            cls.GEOMETRY,
        ]


class OutputColumns:
    """Column names in final output CSV."""

    RLB_ID = "RLB_ID"
    ID = "id"
    NAME = "Name"
    DWELLING_CATEGORY = "Dwel_Cat"
    SOURCE = "Source"
    DWELLINGS = "Dwellings"
    DEV_AREA_HA = "Dev_Area_Ha"
    AREA_IN_NN_CATCHMENT = "AreaInNNCatchment"
    NN_CATCHMENT = "NN_Catchment"
    DEV_SUBCATCHMENT = "Dev_SubCatchment"
    MAJORITY_LPA = "Majority_LPA"
    MAJORITY_WWTW_ID = "Majority_WwTw_ID"
    WWTW_NAME = "WwTW_name"
    WWTW_SUBCATCHMENT = "WwTw_SubCatchment"
    N_LU_UPLIFT = "N_LU_Uplift"
    P_LU_UPLIFT = "P_LU_Uplift"
    N_LU_POST_SUDS = "N_LU_postSuDS"
    P_LU_POST_SUDS = "P_LU_postSuDS"
    OCC_RATE = "Occ_Rate"
    WATER_USAGE_L_DAY = "Water_Usage_L_Day"
    LITRES_USED = "Litres_used"
    NITROGEN_2025_2030 = "Nitrogen_2025_2030"
    NITROGEN_2030_ONWARDS = "Nitrogen_2030_onwards"
    PHOSPHORUS_2025_2030 = "Phosphorus_2025_2030"
    PHOSPHORUS_2030_ONWARDS = "Phosphorus_2030_onwards"
    N_WWTW_TEMP = "N_WwTW_Temp"
    P_WWTW_TEMP = "P_WwTW_Temp"
    N_WWTW_PERM = "N_WwTW_Perm"
    P_WWTW_PERM = "P_WwTW_Perm"
    N_TOTAL = "N_Total"
    P_TOTAL = "P_Total"

    @classmethod
    def final_output_order(cls) -> list[str]:
        return [
            cls.RLB_ID,
            cls.ID,
            cls.NAME,
            cls.DWELLING_CATEGORY,
            cls.SOURCE,
            cls.DWELLINGS,
            cls.DEV_AREA_HA,
            cls.AREA_IN_NN_CATCHMENT,
            cls.NN_CATCHMENT,
            cls.DEV_SUBCATCHMENT,
            cls.MAJORITY_LPA,
            cls.MAJORITY_WWTW_ID,
            cls.WWTW_NAME,
            cls.WWTW_SUBCATCHMENT,
            cls.N_LU_UPLIFT,
            cls.P_LU_UPLIFT,
            cls.N_LU_POST_SUDS,
            cls.P_LU_POST_SUDS,
            cls.OCC_RATE,
            cls.WATER_USAGE_L_DAY,
            cls.LITRES_USED,
            cls.NITROGEN_2025_2030,
            cls.NITROGEN_2030_ONWARDS,
            cls.PHOSPHORUS_2025_2030,
            cls.PHOSPHORUS_2030_ONWARDS,
            cls.N_WWTW_TEMP,
            cls.P_WWTW_TEMP,
            cls.N_WWTW_PERM,
            cls.P_WWTW_PERM,
            cls.N_TOTAL,
            cls.P_TOTAL,
        ]


DEFAULT_CONFIG = AssessmentConfig()


class GcnConfig(BaseSettings):
    """Configuration for GCN (Great Crested Newt) impact assessment."""

    model_config = SettingsConfigDict(
        env_prefix="GCN_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    buffer_distance_m: int = Field(
        default=250, ge=0, description="Buffer distance around RLB (metres)"
    )
    pond_buffer_distance_m: int = Field(
        default=250, ge=0, description="Buffer distance around ponds (metres)"
    )
    merge_distance_m: int = Field(
        default=500,
        ge=0,
        description="Sites within this distance create single buffer (metres)",
    )
    precision_grid_size: float = Field(
        default=0.0001,
        gt=0,
        description="Coordinate precision grid size (metres, 0.1mm matches ArcGIS XY Resolution)",
    )
    target_crs: str = Field(
        default="EPSG:27700",
        description="British National Grid coordinate reference system",
    )


DEFAULT_GCN_CONFIG = GcnConfig()


class DatabaseSettings(BaseSettings):
    """Database connection configuration for PostGIS."""

    model_config = SettingsConfigDict(
        env_prefix="DB_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    host: str = Field(default="localhost", description="Database host")
    port: int = Field(default=5432, description="Database port")
    database: str = Field(default="nrf_impact", description="Database name")
    user: str = Field(default="postgres", description="Database user")

    iam_authentication: bool = Field(
        default=True,
        description="Use IAM authentication for RDS (set to false for local dev)",
    )
    local_password: str = Field(
        default="",
        description="Static password for local development",
    )

    ssl_mode: str = Field(
        default="require",
        description="SSL mode for database connections (require, verify-ca, verify-full)",
    )
    rds_truststore: str = Field(
        default="RDS_ROOT_CA",
        description="Name of TRUSTSTORE_* env var containing RDS CA cert",
    )

    @property
    def connection_url(self) -> str:
        from urllib.parse import quote_plus

        if self.local_password:
            password = quote_plus(self.local_password)
            return f"postgresql://{self.user}:{password}@{self.host}:{self.port}/{self.database}"
        return f"postgresql://{self.user}@{self.host}:{self.port}/{self.database}"  # NOSONAR - intentional: trust auth for local dev without a password


class AWSConfig(BaseSettings):
    """AWS resource configuration for ECS worker deployment."""

    model_config = SettingsConfigDict(
        env_prefix="AWS_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    s3_input_bucket: str = ""
    region: str = Field(default="eu-west-2")
    sqs_queue_url: str = ""
    endpoint_url: str | None = Field(
        default=None, description="Override AWS endpoint for LocalStack"
    )


class ApiServerConfig(BaseSettings):
    """Configuration for the HTTP API server."""

    model_config = SettingsConfigDict(
        env_prefix="API_",
        env_file=".env",
        case_sensitive=False,
        extra="ignore",
    )

    host: str = Field(
        default="127.0.0.1",
        description="Host interface to bind the API server (use 0.0.0.0 for container deployments)",
    )
    port: int = Field(
        default=8085, ge=1, le=65535, description="Port for the API server"
    )
    tracing_header: str = Field(
        default="x-cdp-request-id",
        description="HTTP header name for CDP distributed tracing",
    )
    testing_enabled: bool = Field(
        default=False,
        description="Enable /test endpoints for development (default: false)",
    )
    assess_job_ttl_seconds: int = Field(
        default=3600,
        ge=60,
        description="Time-to-live for /assess job results before cleanup (seconds)",
    )


class DebugConfig:
    """Debug output configuration.

    WARNING: For local development only. Never enable in production.
    """

    def __init__(
        self,
        enabled: bool = False,
        output_dir: Path = Path(tempfile.gettempdir()) / "iat-debug",
    ):
        self.enabled = enabled
        self.output_dir = output_dir

    @classmethod
    def from_env(cls) -> "DebugConfig":
        return cls(
            enabled=os.environ.get("DEBUG_OUTPUT", "false").lower() == "true",
            output_dir=Path(
                os.environ.get(
                    "DEBUG_OUTPUT_DIR", str(Path(tempfile.gettempdir()) / "iat-debug")
                )
            ),
        )
