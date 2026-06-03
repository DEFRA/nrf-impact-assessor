"""add data-sync tracking tables

Matches Liquibase changeset changelog/db.changelog-1.2.xml: adds
data_sync_run (async job records) and data_load_history (per-table audit),
plus a partial unique index enforcing a single in-flight run.

Revision ID: 000000000003
Revises: 000000000002
Create Date: 2026-06-03 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "000000000003"
down_revision: str | Sequence[str] | None = "000000000002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

NOW = sa.text("now()")


def upgrade() -> None:
    op.create_table(
        "data_sync_run",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("data_version", sa.String(), nullable=True),
        sa.Column("forced", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=NOW,
            nullable=False,
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        schema="public",
    )
    op.execute(
        "CREATE UNIQUE INDEX uq_data_sync_run_single_running "
        "ON public.data_sync_run (status) WHERE status = 'running'"
    )

    op.create_table(
        "data_load_history",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("table_name", sa.String(), nullable=False),
        sa.Column("s3_key", sa.String(), nullable=False),
        sa.Column("etag", sa.String(), nullable=False),
        sa.Column("data_version", sa.String(), nullable=True),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column(
            "loaded_at",
            sa.DateTime(timezone=True),
            server_default=NOW,
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["run_id"], ["public.data_sync_run.id"]),
        schema="public",
    )
    op.create_index(
        "ix_data_load_history_table_loaded_at",
        "data_load_history",
        ["table_name", "loaded_at"],
        schema="public",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_data_load_history_table_loaded_at",
        table_name="data_load_history",
        schema="public",
    )
    op.drop_table("data_load_history", schema="public")
    op.execute("DROP INDEX IF EXISTS public.uq_data_sync_run_single_running")
    op.drop_table("data_sync_run", schema="public")
