"""add_performance_indexes

Adds two indexes to eliminate the main query bottlenecks identified in profiling:

1. Composite btree on spatial_layer(layer_type, version)
   The ORM model defines this index but it was never added to the migration.
   Every LATERAL join query filters WHERE layer_type = X AND version = Y — without
   the composite index the planner must bitmap-AND two separate single-column
   indexes. The composite index lets it resolve both columns in a single btree
   scan before touching the GiST.

2. Partial GiST on coefficient_layer(geometry) WHERE version = 1
   The coefficient_layer has 5.4M rows. The land-use intersection query always
   filters on a single version. A partial GiST covering only that version is ~1/N
   the size of the full-table index, making spatial lookups significantly faster.
   Add an equivalent partial index (with version = N) when loading a new version.

Revision ID: 3f8c21a9e047
Revises: 1bf027d04bb3
Create Date: 2026-03-24 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "3f8c21a9e047"
down_revision: str | Sequence[str] | None = "1bf027d04bb3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    # 1. Composite btree index on spatial_layer(layer_type, version).
    #    Defined in the ORM model but missing from previous migrations.
    op.create_index(
        "ix_spatial_layer_type_version",
        "spatial_layer",
        ["layer_type", "version"],
        unique=False,
        schema="nrf_reference",
    )

    # 2. Partial GiST index on coefficient_layer for version 1.
    #    Covers only the active version — a fraction of the 5.4M-row table.
    #    When a new version is loaded, add a matching partial index in a new migration.
    op.execute(
        sa.text(
            "CREATE INDEX ix_nrf_reference_coefficient_layer_geom_v1 "
            "ON nrf_reference.coefficient_layer USING GIST (geometry) "
            "WHERE version = 1"
        )
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.execute(
        sa.text(
            "DROP INDEX IF EXISTS "
            "nrf_reference.ix_nrf_reference_coefficient_layer_geom_v1"
        )
    )
    op.drop_index(
        "ix_spatial_layer_type_version",
        table_name="spatial_layer",
        schema="nrf_reference",
    )
