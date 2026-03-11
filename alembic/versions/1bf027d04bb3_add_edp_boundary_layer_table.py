"""add_edp_boundary_layer_table

Revision ID: 1bf027d04bb3
Revises: 45ce553d0710
Create Date: 2026-03-11 00:00:00.000000

"""

from collections.abc import Sequence

import geoalchemy2
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "1bf027d04bb3"
down_revision: str | Sequence[str] | None = "45ce553d0710"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "edp_boundary_layer",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column(
            "geometry",
            geoalchemy2.types.Geometry(
                geometry_type="GEOMETRY", srid=27700, spatial_index=False
            ),
            nullable=False,
        ),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("attributes", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        schema="nrf_reference",
    )
    op.create_index(
        op.f("ix_nrf_reference_edp_boundary_layer_name"),
        "edp_boundary_layer",
        ["name"],
        unique=False,
        schema="nrf_reference",
    )
    op.create_index(
        op.f("ix_nrf_reference_edp_boundary_layer_version"),
        "edp_boundary_layer",
        ["version"],
        unique=False,
        schema="nrf_reference",
    )
    op.execute(
        sa.text(
            "CREATE INDEX ix_nrf_reference_edp_boundary_layer_geometry "
            "ON nrf_reference.edp_boundary_layer USING GIST (geometry)"
        )
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(
        "ix_nrf_reference_edp_boundary_layer_geometry",
        table_name="edp_boundary_layer",
        schema="nrf_reference",
    )
    op.drop_index(
        op.f("ix_nrf_reference_edp_boundary_layer_version"),
        table_name="edp_boundary_layer",
        schema="nrf_reference",
    )
    op.drop_index(
        op.f("ix_nrf_reference_edp_boundary_layer_name"),
        table_name="edp_boundary_layer",
        schema="nrf_reference",
    )
    op.drop_table("edp_boundary_layer", schema="nrf_reference")