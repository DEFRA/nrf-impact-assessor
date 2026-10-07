"""add audit_model_results; drop coefficient_layer descriptive columns

Two independent changes, merged into one revision because they ship together.

1. `audit_model_results` is added: one row per assessment run — status, NRL
   reference, assessment type, the model's output as JSONB, the model (git)
   version, and the error on failure.

2. `land_use_cat`, `nn_catchment` and `subcatchment` are dropped from
   `coefficient_layer`. The England v2 coefficient layer
   (NMSCoefficientLayer_England_v2_IAT.gpkg) publishes only cromeid and the four
   coefficients, and the assessment never read the three descriptive columns, so
   they are dropped rather than carried as permanently NULL. Dropping a column in
   PostgreSQL is a catalogue change, so this is fast even on a fully loaded
   table; the indexes on nn_catchment and subcatchment go with their columns.
   Downgrade restores the columns and indexes empty: their values are not
   recoverable without reloading an older coefficient GeoPackage.

Revision ID: e5f9a2b3c4d6
Revises: d4e8f1a2b3c5
Create Date: 2026-10-05 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "e5f9a2b3c4d6"
down_revision: str | Sequence[str] | None = "d4e8f1a2b3c5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TABLE = "audit_model_results"
COEFFICIENT_TABLE = "coefficient_layer"
COEFFICIENT_DROPPED_COLUMNS = ("land_use_cat", "nn_catchment", "subcatchment")
COEFFICIENT_DROPPED_INDEXED_COLUMNS = ("nn_catchment", "subcatchment")


def upgrade() -> None:
    op.create_table(
        TABLE,
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("nrl_reference", sa.String(), nullable=False),
        sa.Column("assessment_type", sa.String(), nullable=False),
        sa.Column("model_version", sa.String(), nullable=False),
        sa.Column(
            "model_output", postgresql.JSONB(astext_type=sa.Text()), nullable=True
        ),
        sa.Column("error_details", sa.String(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        schema="public",
    )
    op.create_index(
        "ix_public_audit_model_results_nrl_reference",
        TABLE,
        ["nrl_reference"],
        schema="public",
    )

    for column in COEFFICIENT_DROPPED_COLUMNS:
        op.drop_column(COEFFICIENT_TABLE, column, schema="public")


def downgrade() -> None:
    for column in COEFFICIENT_DROPPED_COLUMNS:
        op.add_column(
            COEFFICIENT_TABLE,
            sa.Column(column, sa.String(), nullable=True),
            schema="public",
        )
    for column in COEFFICIENT_DROPPED_INDEXED_COLUMNS:
        op.create_index(
            f"ix_public_{COEFFICIENT_TABLE}_{column}",
            COEFFICIENT_TABLE,
            [column],
            schema="public",
        )

    op.execute(f"DROP TABLE IF EXISTS public.{TABLE} CASCADE")
