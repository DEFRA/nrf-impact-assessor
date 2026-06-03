"""Parsing and validation for the S3 reference-data manifest."""

from typing import Any

from pydantic import BaseModel, field_validator


class Manifest(BaseModel):
    """Declarative description of one reference-data version in S3."""

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


def parse_manifest(raw: dict[str, Any]) -> Manifest:
    """Build a Manifest from a decoded JSON dict, raising ValueError if invalid."""
    if "data_version" not in raw:
        msg = "manifest missing required key: data_version"
        raise ValueError(msg)
    if "tables" not in raw:
        msg = "manifest missing required key: tables"
        raise ValueError(msg)
    return Manifest.model_validate(raw)
