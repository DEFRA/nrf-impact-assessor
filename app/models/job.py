"""Job message schema for SQS-based worker coordination."""

from datetime import UTC, datetime

from pydantic import BaseModel, EmailStr, Field

from app.models.enums import AssessmentType


class IntersectingEdp(BaseModel):
    """An EDP that intersects with the development boundary."""

    label: str
    n2k_site_name: str


class BoundaryGeojson(BaseModel):
    """Boundary geometry and intersecting EDPs from nrf-backend."""

    boundary_geometry_original: dict = Field(alias="boundaryGeometryOriginal")
    intersecting_edps: list[IntersectingEdp] = Field(alias="intersectingEdps")

    model_config = {"populate_by_name": True}


class ImpactAssessmentJob(BaseModel):
    """SQS message schema for impact assessment jobs.

    Supports two message formats:

    1. Quote payload from nrf-backend (via SNS):
       - reference, boundaryGeojson, developmentTypes, residentialBuildingCount,
         peopleCount, email, wasteWaterTreatmentWorksId/Name

    2. Legacy direct SQS message (for tests):
       - job_id, s3_input_key, developer_email, dwelling_type,
         number_of_dwellings, assessment_type

    All fields are optional to accept both formats. The orchestrator
    determines the geometry source: boundaryGeojson (inline) or
    s3_input_key (S3 download).
    """

    # --- Quote payload fields (from nrf-backend POST /quotes) ---
    reference: str | None = Field(default=None, pattern=r"^NRF-\d{6}$")
    boundary_geojson: BoundaryGeojson | None = Field(
        default=None, alias="boundaryGeojson"
    )
    boundary_entry_type: str | None = Field(default=None, alias="boundaryEntryType")
    development_types: list[str] | None = Field(default=None, alias="developmentTypes")
    residential_building_count: int | None = Field(
        default=None, ge=1, alias="residentialBuildingCount"
    )
    people_count: int | None = Field(default=None, ge=1, alias="peopleCount")
    waste_water_treatment_works_id: str | None = Field(
        default=None, alias="wasteWaterTreatmentWorksId"
    )
    waste_water_treatment_works_name: str | None = Field(
        default=None, alias="wasteWaterTreatmentWorksName"
    )
    email: EmailStr | None = Field(default=None)

    # --- Legacy fields (for S3-based processing and tests) ---
    job_id: str | None = Field(default=None, description="Unique job identifier")
    s3_input_key: str | None = Field(
        default=None, description="S3 key to input shapefile zip"
    )
    geometry: str | None = Field(
        default=None, description="GeoJSON string for test enqueue endpoint"
    )
    developer_email: EmailStr | None = Field(default=None)
    submitted_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    development_name: str = Field(default="")
    dwelling_type: str | None = Field(default=None)
    number_of_dwellings: int | None = Field(default=None, ge=1)
    assessment_type: AssessmentType | None = Field(default=None)

    model_config = {
        "populate_by_name": True,
        "json_schema_extra": {
            "example": {
                "reference": "NRF-000001",
                "boundaryEntryType": "draw",
                "boundaryGeojson": {
                    "boundaryGeometryOriginal": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [582814.93, 328188.89],
                                [582808.89, 328203.73],
                                [582824.96, 328210.17],
                                [582830.09, 328197.00],
                                [582814.93, 328188.89],
                            ]
                        ],
                    },
                    "intersectingEdps": [
                        {
                            "label": "River Wensum SAC",
                            "n2k_site_name": "River Wensum SAC",
                        }
                    ],
                },
                "developmentTypes": ["housing"],
                "residentialBuildingCount": 25,
                "email": "developer@example.com",
            }
        },
    }

    @property
    def effective_id(self) -> str:
        """Return the best available identifier for this job."""
        return self.job_id or self.reference or "unknown"

    @property
    def effective_dwelling_type(self) -> str:
        """Return dwelling type from either message format."""
        if self.dwelling_type:
            return self.dwelling_type
        if self.development_types:
            return self.development_types[0]
        return "housing"

    @property
    def effective_dwellings(self) -> int:
        """Return dwelling count from either message format."""
        return self.number_of_dwellings or self.residential_building_count or 0
