"""Job message schema for SQS-based worker coordination."""

from pydantic import BaseModel, Field


class IntersectingEdp(BaseModel):
    """An EDP that intersects with the development boundary.

    `/check-boundary` no longer emits `n2k_site_name` (it duplicated `label`)
    or the EDP/intersection polygons. Pydantic ignores unknown keys, so
    messages queued before that change still parse.
    """

    label: str


class BoundaryGeojson(BaseModel):
    """Boundary geometry and intersecting EDPs from nrf-backend."""

    boundary_geometry_original: dict = Field(alias="boundaryGeometryOriginal")
    intersecting_edps: list[IntersectingEdp] = Field(alias="intersectingEdps")

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
    development_types: list[str] | None = Field(default=None, alias="developmentTypes")
    residential_building_count: int | None = Field(
        default=None, ge=1, alias="housingUnits"
    )
    people_count: int | None = Field(default=None, ge=1, alias="peopleCount")
    waste_water_treatment_works_id: str | None = Field(
        default=None, alias="wasteWaterTreatmentWorksId"
    )
    trace_id: str | None = Field(default=None, alias="traceId")

    model_config = {
        "populate_by_name": True,
        "json_schema_extra": {
            "example": {
                "reference": "NRF-000001",
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
                    "intersectingEdps": [{"label": "River Wensum SAC"}],
                },
                "developmentTypes": ["housing"],
                "housingUnits": 25,
            }
        },
    }
