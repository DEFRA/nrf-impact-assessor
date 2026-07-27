"""add data_load_history.row_version

Revision ID: c3d7e1f2a4b6
Revises: b2e5f0a1c9d4
Create Date: 2026-07-25 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c3d7e1f2a4b6"
down_revision: str | Sequence[str] | None = "b2e5f0a1c9d4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

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
BEGIN
    FOR t IN
        SELECT DISTINCT table_name
        FROM public.data_load_history
        WHERE status IN ('success', 'reconciled')
    LOOP
        CONTINUE WHEN to_regclass('public.' || quote_ident(t)) IS NULL;
        CONTINUE WHEN NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = t
              AND column_name = 'version'
        );
        EXECUTE format('SELECT MAX(version) FROM public.%I', t) INTO maxv;
        CONTINUE WHEN maxv IS NULL;
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


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("data_load_history", "row_version", schema="public")
