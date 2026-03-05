"""Job message schema for SQS-based worker coordination."""

from datetime import UTC, datetime

from pydantic import BaseModel, EmailStr, Field

from app.models.enums import AssessmentType


class ImpactAssessmentJob(BaseModel):
    """SQS message schema for impact assessment jobs.

    Attributes:
        job_id: Unique identifier for this assessment job
        s3_input_key: S3 key to shapefile zip (e.g., "jobs/abc123/input.zip")
        developer_email: Email address of developer submitting the job
        submitted_at: ISO 8601 timestamp when job was submitted
        development_name: Optional name/description for the development
        dwelling_type: Dwelling type from developer form submission.
            Will become Enum once final list is determined.
        number_of_dwellings: Number of dwellings from developer form submission.
        assessment_type: The type of assessment to run (e.g., "nutrient", "gcn")

    ** Note ** These fields are not finalised yet

    Note:
        In production, geometry file contains only geometry. All development data
        (development_name, dwelling_type, number_of_dwellings) comes from the frontend form.
        EmbeddedDevelopmentDataValidator is only for legacy test data with attributes.
    """

    job_id: str = Field(..., description="Unique job identifier")
    s3_input_key: str = Field(..., description="S3 key to input shapefile zip")
    developer_email: EmailStr = Field(..., description="Developer's email address")
    submitted_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    development_name: str = Field(
        default="",
        description="Optional name/description for the development",
    )
    dwelling_type: str = Field(
        ...,
        description="Dwelling type from form (will become Enum once finalized)",
    )
    number_of_dwellings: int = Field(
        ...,
        ge=1,
        description="Number of dwellings from form",
    )
    assessment_type: AssessmentType = Field(
        ..., description="Type of assessment to run"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "job_id": "550e8400-e29b-41d4-a716-446655440000",
                "s3_input_key": "jobs/550e8400/input.zip",
                "developer_email": "developer@example.com",
                "submitted_at": "2025-10-15T14:30:00Z",
                "development_name": "Big homes",
                "dwelling_type": "apartment",
                "number_of_dwellings": 25,
                "assessment_type": "nutrient",
            }
        }
    }
