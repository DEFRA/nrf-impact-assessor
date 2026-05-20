"""add per-layer spatial tables

Matches Liquibase changeset 01-initial-schema.xml: adds dedicated tables for
each spatial layer type (wwtw_catchments, lpa_boundaries, nn_catchments,
subcatchments, gcn_risk_zones, gcn_ponds, edp_edges).

Revision ID: 000000000002
Revises: 000000000001
Create Date: 2026-05-18 00:00:00.000000
"""

from collections.abc import Sequence

import geoalchemy2
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "000000000002"
down_revision: str | Sequence[str] | None = "000000000001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

NOW = sa.text("now()")

LAYER_TABLES = (
    "wwtw_catchments",
    "lpa_boundaries",
    "nn_catchments",
    "subcatchments",
    "gcn_risk_zones",
    "gcn_ponds",
    "edp_edges",
)


def upgrade() -> None:
    for table in LAYER_TABLES:
        op.create_table(
            table,
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
            sa.Column(
                "attributes", postgresql.JSONB(astext_type=sa.Text()), nullable=True
            ),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                server_default=NOW,
                nullable=False,
            ),
            sa.PrimaryKeyConstraint("id"),
            schema="public",
        )
        op.create_index(
            f"ix_public_{table}_version",
            table,
            ["version"],
            schema="public",
        )
        op.create_index(
            f"ix_public_{table}_name",
            table,
            ["name"],
            schema="public",
        )
        op.execute(
            sa.text(
                f"CREATE INDEX ix_public_{table}_geometry "
                f"ON public.{table} USING GIST (geometry)"
            )
        )


def downgrade() -> None:
    for table in reversed(LAYER_TABLES):
        op.execute(f"DROP TABLE IF EXISTS public.{table} CASCADE")
