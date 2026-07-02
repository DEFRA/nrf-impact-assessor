"""add data_load_history.status_detail

Revision ID: 94839dfef894
Revises: 000000000003
Create Date: 2026-07-02 00:40:49.370327

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "94839dfef894"
down_revision: str | Sequence[str] | None = "000000000003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "data_load_history",
        sa.Column("status_detail", sa.String(), nullable=True),
        schema="public",
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("data_load_history", "status_detail", schema="public")
