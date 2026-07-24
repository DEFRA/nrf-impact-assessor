"""add edp_excluded_areas spatial table

Adds the edp_excluded_areas reference layer (buffered SSSI exclusion polygons).
Mirrors the per-layer table shape from 000000000002_add_per_layer_tables.py.

Revision ID: b2e5f0a1c9d4
Revises: a1c4f9d02b7e
Create Date: 2026-07-13 00:00:00.000000
"""

from collections.abc import Sequence

import geoalchemy2
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "b2e5f0a1c9d4"
down_revision: str | Sequence[str] | None = "a1c4f9d02b7e"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

NOW = sa.text("now()")
TABLE = "edp_excluded_areas"


def upgrade() -> None:
    op.create_table(
        TABLE,
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column(
            "geometry",
            geoalchemy2.types.Geometry(
                geometry_type="GEOMETRY",
                srid=27700,
                spatial_index=False,
            ),
            nullable=False,
        ),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("attributes", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=NOW,
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        schema="public",
    )
    op.create_index(f"ix_public_{TABLE}_version", TABLE, ["version"], schema="public")
    op.create_index(f"ix_public_{TABLE}_name", TABLE, ["name"], schema="public")
    op.execute(
        sa.text(
            f"CREATE INDEX ix_public_{TABLE}_geometry "
            f"ON public.{TABLE} USING GIST (geometry)"
        )
    )


def downgrade() -> None:
    op.execute(f"DROP TABLE IF EXISTS public.{TABLE} CASCADE")
