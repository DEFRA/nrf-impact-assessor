"""Request schema describing one reference-data version to load."""

from pydantic import BaseModel, field_validator


class Manifest(BaseModel):
    """Declarative description of one reference-data version.

    Supplied in the body of a POST /admin/data-sync call: the version string
    and the map of table name -> dump object key (relative to the configured
    S3 prefix) to restore.
    """

    data_version: str
    tables: dict[str, str]  # table_name -> dump object key (relative to prefix)

    @field_validator("data_version")
    @classmethod
    def _non_empty_version(cls, v: str) -> str:
        if not v:
            msg = "manifest data_version must be a non-empty string"
            raise ValueError(msg)
        return v

    @field_validator("tables")
    @classmethod
    def _non_empty_tables(cls, v: dict[str, str]) -> dict[str, str]:
        if not v:
            msg = "manifest tables map must contain at least one entry"
            raise ValueError(msg)
        return v
