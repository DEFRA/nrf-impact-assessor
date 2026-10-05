"""add audit_model_results

One row per assessment run: status, NRL reference, assessment type, the
model's output as JSONB, the model (git) version, and the error on failure.

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


def downgrade() -> None:
    op.execute(f"DROP TABLE IF EXISTS public.{TABLE} CASCADE")
