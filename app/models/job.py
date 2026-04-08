"""Job message schema for SQS-based worker coordination."""

from pydantic import BaseModel, EmailStr, Field


class IntersectingEdp(BaseModel):
    """An EDP that intersects with the development boundary."""

    label: str
    n2k_site_name: str


class BoundaryGeojson(BaseModel):
    """Boundary geometry and intersecting EDPs from nrf-backend."""

    boundary_geometry_original: dict = Field(alias="boundaryGeometryOriginal")
    intersecting_edps: list[IntersectingEdp] = Field(alias="intersectingEdps")

    model_config = {"populate_by_name": True}


class LevyRange(BaseModel):
    """Levy amount range in GBP."""

    min: float = Field(ge=0)
    max: float = Field(ge=0)


class EdpInput(BaseModel):
    """EDP metadata from nrf-backend for result callback."""

    edp_id: int = Field(alias="edpId")
    edp_name: str = Field(alias="edpName")
    edp_type: str = Field(default="NUTRIENT", alias="edpType")
    levy_gbp: LevyRange = Field(alias="levyGbp")

    model_config = {"populate_by_name": True}


class ImpactAssessmentJob(BaseModel):
    """SQS message schema for impact assessment jobs.

    Quote payload from nrf-backend, delivered via SNS → SQS. `boundaryGeojson`
    is required; all other fields are optional so malformed messages can still
    be parsed for logging/DLQ.
    """

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

    # EDP metadata for result callback to nrf-backend
    edps: list["EdpInput"] | None = Field(default=None)

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
