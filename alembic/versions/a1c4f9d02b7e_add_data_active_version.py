"""add data_active_version and data_rollback_event (DM-4)

Matches Liquibase changeset changelog/db.changelog-1.5.xml.

Revision ID: a1c4f9d02b7e
Revises: 94839dfef894
Create Date: 2026-07-02 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "a1c4f9d02b7e"
down_revision: str | Sequence[str] | None = "94839dfef894"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

NOW = sa.text("now()")


def upgrade() -> None:
    op.create_table(
        "data_active_version",
        sa.Column("table_name", sa.String(), nullable=False),
        sa.Column("active_version", sa.Integer(), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=NOW,
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("table_name"),
        schema="public",
    )

    op.create_table(
        "data_rollback_event",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("table_name", sa.String(), nullable=False),
        sa.Column("from_version", sa.Integer(), nullable=False),
        sa.Column("to_version", sa.Integer(), nullable=False),
        sa.Column(
            "rolled_back_at",
            sa.DateTime(timezone=True),
            server_default=NOW,
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        schema="public",
    )


def downgrade() -> None:
    op.drop_table("data_rollback_event", schema="public")
    op.drop_table("data_active_version", schema="public")
