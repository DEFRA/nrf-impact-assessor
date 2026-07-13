"""Pydantic schema and loader for the DM-2-confirmed QC rule thresholds.

The rules and thresholds here are the single machine-readable copy of the
sign-off recorded in `docs/data-management.md` ("Confirmed default QC rules,
DM-2, 2026-06-30"). That document is the human-readable description of what
lives here — this file is the source of truth `qc.py` builds SQL from.
"""

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field

_DEFAULT_PATH = Path(__file__).parent / "qc_rules.yaml"


class CoefficientRange(BaseModel):
    """Inclusive numeric bounds for one `coefficient_layer` column (rule 7)."""

    min: float
    max: float


class KeyRule(BaseModel):
    """Declared business key for rules 3 (non-null) and 8 (uniqueness)."""

    columns: list[str]
    source: Literal["column", "json"]
    unique: bool = False


class LookupRowRule(BaseModel):
    """Key column (inside a JSONB array element) for one `lookup_table` row."""

    json_key: str


class GeometryRule(BaseModel):
    """Expected geometry shape for rules 4-6, grounded in the production dumps."""

    expected_type: str
    expected_srid: int = 27700


class TableRules(BaseModel):
    """All applicable QC rules for one reference table."""

    row_count_floor_pct: float | None = None
    key: KeyRule | None = None
    non_null_columns: list[str] = Field(default_factory=list)
    non_null_json_columns: list[str] = Field(default_factory=list)
    allowed_values: dict[str, list[str]] = Field(default_factory=dict)
    coefficient_ranges: dict[str, CoefficientRange] = Field(default_factory=dict)
    lookup_rows: dict[str, LookupRowRule] = Field(default_factory=dict)
    geometry: GeometryRule | None = None


class ReferentialSide(BaseModel):
    """One side of a cross-table referential check (rule 9)."""

    table: str
    column: str | None = None
    lookup_row: str | None = None
    json_key: str | None = None


class ReferentialCheck(BaseModel):
    """A confirmed cross-table referential pair (rule 9)."""

    model_config = ConfigDict(populate_by_name=True)

    name: str
    from_: ReferentialSide = Field(alias="from")
    to: ReferentialSide
    numeric_coercion: bool = False
    allow_null_from: bool = False


class QcRules(BaseModel):
    """The full set of DM-2-confirmed QC rules."""

    row_count_floor_pct: float = 90
    tables: dict[str, TableRules]
    referential_checks: list[ReferentialCheck] = Field(default_factory=list)


def load_qc_rules(path: Path = _DEFAULT_PATH) -> QcRules:
    """Load and validate the QC rules YAML at `path`."""
    with path.open() as f:
        raw = yaml.safe_load(f)
    return QcRules.model_validate(raw)
