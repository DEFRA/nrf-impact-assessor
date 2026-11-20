"""add levy_inflation_index_history

One row per inflation index addition, written in the same transaction as
the add. Amendments and deletions are not recorded. The table is new and
starts empty: the seeded rows predate admin additions, so there is no
backfill — the history begins when the table does.

inflation_index_id has no foreign key on purpose: a deleted index must keep
its history.

Revision ID: a3f6c8d2e4b7
Revises: e05a352f9ebe
Create Date: 2026-11-18 09:30:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "a3f6c8d2e4b7"
down_revision: str | Sequence[str] | None = "e05a352f9ebe"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TABLE = "levy_inflation_index_history"
NOW_SQL = "now()"


def upgrade() -> None:
    op.create_table(
        TABLE,
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("inflation_index_id", sa.Uuid(), nullable=False),
        sa.Column("charging_year", sa.Integer(), nullable=False),
        sa.Column("cil_index", sa.Numeric(10, 4), nullable=False),
        sa.Column("index_factor", sa.Numeric(10, 4), nullable=False),
        sa.Column("comment", sa.String(500), nullable=False),
        sa.Column("staff_id", sa.String(36), nullable=False),
        sa.Column("staff_full_name", sa.String(255), nullable=False),
        sa.Column("staff_email", sa.String(254), nullable=False),
        sa.Column("requestor", sa.String(255), nullable=True),
        sa.Column("jira_ticket", sa.String(20), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text(NOW_SQL),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            r"comment ~ '\S'",
            name="ck_levy_inflation_index_history_comment_not_blank",
        ),
        schema="public",
    )
    op.create_index(
        f"ix_public_{TABLE}_inflation_index_id",
        TABLE,
        ["inflation_index_id"],
        schema="public",
    )


def downgrade() -> None:
    op.drop_index(f"ix_public_{TABLE}_inflation_index_id", TABLE, schema="public")
    op.drop_table(TABLE, schema="public")
