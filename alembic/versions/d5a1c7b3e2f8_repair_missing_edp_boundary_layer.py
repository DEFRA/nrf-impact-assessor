"""repair pre-squash schema drift

Two fixes for databases that predate the squashed baseline.

1. `public.edp_boundary_layer` is recreated where missing. The `000000000001`
   baseline is a squash of four earlier revisions, one of which
   is a squash of four earlier revisions, one of which (`1bf027d04bb3`) created
   that table. Databases predating the squash were stamped onto the baseline
   rather than running it, so the table was never created — and because nothing
   downstream recreates it, they sit at head permanently missing it while
   `alembic current` reports everything is applied.

2. `public.spatial_layer` is dropped. It was the original shared table for every
   spatial layer; revision `000000000002` replaced it with a dedicated table per
   layer but never dropped it. Nothing in the application reads or writes it —
   there is no model mapped to it and no query references it.

Both steps are conditional, so this repairs a stamped database and is a no-op on
a correctly migrated one. Nothing that is still in use is dropped or rewritten.

Revision ID: d5a1c7b3e2f8
Revises: c3d7e1f2a4b6
Create Date: 2026-07-29 00:00:00.000000
"""

from collections.abc import Sequence

import geoalchemy2
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "d5a1c7b3e2f8"
down_revision: str | Sequence[str] | None = "c3d7e1f2a4b6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

NOW = sa.text("now()")

TABLE = "edp_boundary_layer"


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if TABLE not in inspector.get_table_names(schema="public"):
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

    # Named separately from the table check: a database repaired by hand may
    # have the table but not the baseline's index names.
    op.execute(
        sa.text(
            "CREATE INDEX IF NOT EXISTS ix_public_edp_boundary_layer_name "
            f"ON public.{TABLE} (name)"
        )
    )
    op.execute(
        sa.text(
            "CREATE INDEX IF NOT EXISTS ix_public_edp_boundary_layer_version "
            f"ON public.{TABLE} (version)"
        )
    )
    op.execute(
        sa.text(
            "CREATE INDEX IF NOT EXISTS ix_public_edp_boundary_layer_geometry "
            f"ON public.{TABLE} USING GIST (geometry)"
        )
    )

    op.execute("DROP TABLE IF EXISTS public.spatial_layer CASCADE")


def downgrade() -> None:
    """No-op: edp_boundary_layer belongs to the baseline, so dropping it here
    would destroy data a correctly migrated database has always had, and
    recreating the unmapped legacy spatial_layer empty would restore nothing.
    """
