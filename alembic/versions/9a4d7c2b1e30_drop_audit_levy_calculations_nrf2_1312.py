"""drop audit_levy_calculations (NRF2-1312)

The levy calculation history now lives in nrf-backend's quote_edp_results,
which holds every audit field this table did, so the impact assessor no longer
records or stamps rows. Existing rows are discarded: there is no production
data yet.

downgrade() recreates the table empty, as it was before this revision:
levy_charge_id an integer with no foreign key, the two index columns
RESTRICT-bound to levy_inflation_index.

Revision ID: 9a4d7c2b1e30
Revises: a3f6c8d2e4b7
Create Date: 2026-11-17 12:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "9a4d7c2b1e30"
down_revision: str | Sequence[str] | None = "a3f6c8d2e4b7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TABLE = "audit_levy_calculations"
INDEX_TABLE = "public.levy_inflation_index.id"


def upgrade() -> None:
    op.drop_table(TABLE, schema="public")


def downgrade() -> None:
    op.create_table(
        TABLE,
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("quote_reference", sa.String(), nullable=False),
        sa.Column("edp_id", sa.Integer(), nullable=False),
        sa.Column("edp_name", sa.String(), nullable=False),
        sa.Column("edp_start_date", sa.Date(), nullable=False),
        sa.Column("calculator_version", sa.Integer(), nullable=False),
        sa.Column("base_charge_per_unit", sa.Numeric(12, 4), nullable=False),
        sa.Column("units", sa.Integer(), nullable=False),
        sa.Column("calculation_date", sa.Date(), nullable=False),
        sa.Column("provisional_amount", sa.Numeric(12, 2), nullable=False),
        sa.Column("inflation_adjusted_amount", sa.Numeric(12, 2), nullable=False),
        sa.Column("levy_charge_id", sa.Integer(), nullable=True),
        sa.Column("edp_start_year_index_id", postgresql.UUID(as_uuid=True)),
        sa.Column("edp_start_year_index_factor", sa.Numeric(10, 4)),
        sa.Column("calculation_year_index_id", postgresql.UUID(as_uuid=True)),
        sa.Column("calculation_year_index_factor", sa.Numeric(10, 4)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("sent_at", sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(
            ["edp_start_year_index_id"],
            [INDEX_TABLE],
            name="fk_audit_levy_calculations_edp_start_year_index",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["calculation_year_index_id"],
            [INDEX_TABLE],
            name="fk_audit_levy_calculations_calculation_year_index",
            ondelete="RESTRICT",
        ),
        schema="public",
    )
    op.create_index(
        "ix_public_audit_levy_calculations_quote_reference",
        TABLE,
        ["quote_reference"],
        schema="public",
    )
    op.create_index(
        "ix_public_audit_levy_calculations_levy_charge_id",
        TABLE,
        ["levy_charge_id"],
        schema="public",
    )
