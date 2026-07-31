"""add data_load_history.row_version; repair pre-squash schema drift

Two independent changes, merged into one revision because they ship together.

1. `data_load_history.row_version` is added and backfilled. Every lookup that
   resolves "which manifest version is live" joins on it exactly, so without the
   backfill provenance reads empty until each table is re-synced.

2. Pre-squash schema drift is repaired. `public.edp_boundary_layer` is recreated
   where missing: the `000000000001` baseline is a squash of four earlier
   revisions, one of which (`1bf027d04bb3`) created that table. Databases
   predating the squash were stamped onto the baseline rather than running it,
   so the table was never created — and because nothing downstream recreates it,
   they sit at head permanently missing it while `alembic current` reports
   everything is applied. `public.spatial_layer` is also dropped: it was the
   original shared table for every spatial layer, replaced by a dedicated table
   per layer in `000000000002` but never removed. Nothing in the application
   reads or writes it — no model maps it and no query references it. The drop is
   deliberately not CASCADE, so an unknown dependent aborts the migration rather
   than being destroyed with it.

Every step of (2) is conditional, so this repairs a stamped database and is a
no-op on a correctly migrated one. Nothing still in use is dropped or rewritten.

Revision ID: c3d7e1f2a4b6
Revises: b2e5f0a1c9d4
Create Date: 2026-07-25 00:00:00.000000

"""

from collections.abc import Sequence

import geoalchemy2
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c3d7e1f2a4b6"
down_revision: str | Sequence[str] | None = "b2e5f0a1c9d4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

NOW = sa.text("now()")

TABLE = "edp_boundary_layer"

# Rows written before this column existed have row_version NULL, and every
# lookup that resolves "which manifest version is live" joins on it exactly
# (service.active_applied_version / resolve_active_provenance). Without a
# backfill those joins match nothing, so provenance reads empty until each
# table is re-synced — and with per-table subset syncs a table absent from the
# next manifest would stay empty indefinitely.
#
# Reconstruction: retention keeps MAX(version) and MAX(version)-1, and each load
# increments the version, so the newest history row for a table corresponds to
# its current MAX(version), the one before it to MAX-1, and so on backwards.
# Ranking success/reconciled rows by loaded_at DESC and counting down from
# MAX(version) therefore reproduces exactly what post_sql would have stamped.
# Rows that would land below version 1 are left NULL (their data is long gone).
#
# Driven off the table names present in data_load_history rather than a
# hard-coded registry, skipping any that no longer exist or lack a `version`
# column, so the migration stays valid as the reference-table set changes.
_BACKFILL_SQL = """
DO $backfill$
DECLARE
    t text;
    maxv integer;
    updated integer;
    candidates integer;
BEGIN
    FOR t IN
        SELECT DISTINCT table_name
        FROM public.data_load_history
        WHERE status IN ('success', 'reconciled')
    LOOP
        IF to_regclass('public.' || quote_ident(t)) IS NULL THEN
            RAISE NOTICE 'row_version backfill: skipped % (table does not exist)', t;
            CONTINUE;
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = t
              AND column_name = 'version'
        ) THEN
            RAISE NOTICE 'row_version backfill: skipped % (no version column)', t;
            CONTINUE;
        END IF;
        EXECUTE format('SELECT MAX(version) FROM public.%I', t) INTO maxv;
        IF maxv IS NULL THEN
            RAISE NOTICE 'row_version backfill: skipped % (table is empty)', t;
            CONTINUE;
        END IF;
        SELECT count(*) INTO candidates
        FROM public.data_load_history
        WHERE table_name = t
          AND status IN ('success', 'reconciled')
          AND row_version IS NULL;
        UPDATE public.data_load_history h
        SET row_version = maxv - (r.rn - 1)
        FROM (
            SELECT id,
                   row_number() OVER (ORDER BY loaded_at DESC, id DESC) AS rn
            FROM public.data_load_history
            WHERE table_name = t
              AND status IN ('success', 'reconciled')
        ) r
        WHERE h.id = r.id
          AND h.row_version IS NULL
          AND maxv - (r.rn - 1) >= 1;
        GET DIAGNOSTICS updated = ROW_COUNT;
        RAISE NOTICE 'row_version backfill: % stamped %/% rows, max(version)=%',
            t, updated, candidates, maxv;
        IF updated < candidates THEN
            RAISE NOTICE 'row_version backfill: % left % row(s) NULL '
                '(would fall below version 1; their data is no longer retained)',
                t, candidates - updated;
        END IF;
    END LOOP;
END $backfill$;
"""


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "data_load_history",
        sa.Column("row_version", sa.Integer(), nullable=True),
        schema="public",
    )
    op.execute(_BACKFILL_SQL)

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

    # No CASCADE. Nothing should depend on this table — it is mapped to no
    # model and referenced by no query — but this migration exists precisely
    # because some databases drifted in ways we did not predict. A plain DROP
    # fails loudly on an unknown dependent view or foreign key instead of
    # quietly destroying it; a migration that errors is recoverable, silently
    # dropped dependencies are not.
    op.execute("DROP TABLE IF EXISTS public.spatial_layer")


def downgrade() -> None:
    """Reverse only the row_version column.

    The repair half is deliberately not reversed: edp_boundary_layer belongs to
    the baseline, so dropping it here would destroy data a correctly migrated
    database has always had, and recreating the unmapped legacy spatial_layer
    empty would restore nothing.
    """
    op.drop_column("data_load_history", "row_version", schema="public")
