"""add levy_charges, audit_levy_calculations and levy_inflation_index

Base charge per EDP and charging year, the audit record for every levy
calculation (NRF2-913), and the published RICS CIL Index per charging year
with the factor it derives against the 2026 base (2026 = 1.0000).

Each audit row names the levy_charges and levy_inflation_index rows it used
(RESTRICT foreign keys) alongside the copied values, so a later correction to
a row can be told apart from the wrong row being picked up.

Squashed into one revision because all three tables were still unmerged.

No rows are seeded: finance has not confirmed a base charge for any EDP,
and seeding an unconfirmed value would let a real quote calculate against it
(scenario 5 requires no assumed/default value). The RICS CIL Index rows are
loaded separately rather than baked into this migration.

Revision ID: d4e8f1a2b3c5
Revises: c3d7e1f2a4b6
Create Date: 2026-09-14 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "d4e8f1a2b3c5"
down_revision: str | Sequence[str] | None = "c3d7e1f2a4b6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

CHARGES_TABLE = "levy_charges"
CALCULATIONS_TABLE = "audit_levy_calculations"
INFLATION_INDEX_TABLE = "levy_inflation_index"
NOW_SQL = "now()"


def upgrade() -> None:
    # Integer equality inside a GiST exclusion constraint needs btree_gist.
    op.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")
    op.create_table(
        CHARGES_TABLE,
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("edp_id", sa.Integer(), nullable=False),
        sa.Column("edp_name", sa.String(), nullable=False),
        sa.Column("edp_start_date", sa.Date(), nullable=False),
        sa.Column("charge_valid_from", sa.Date(), nullable=False),
        sa.Column("charge_valid_to", sa.Date(), nullable=False),
        sa.Column("base_charge_per_unit", sa.Numeric(12, 4), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text(NOW_SQL),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            "charge_valid_to >= charge_valid_from",
            name="ck_levy_charges_valid_window",
        ),
        schema="public",
    )
    # One charge per EDP per day: two overlapping windows would leave the
    # lookup to choose between prices.
    op.execute(
        f"ALTER TABLE public.{CHARGES_TABLE} "
        "ADD CONSTRAINT ex_levy_charges_edp_window EXCLUDE USING gist "
        "(edp_id WITH =, "
        "daterange(charge_valid_from, charge_valid_to, '[]') WITH &&)"
    )
    op.create_index(
        f"ix_public_{CHARGES_TABLE}_edp_id", CHARGES_TABLE, ["edp_id"], schema="public"
    )

    op.create_table(
        INFLATION_INDEX_TABLE,
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("charging_year", sa.Integer(), nullable=False),
        sa.Column("cil_index", sa.Numeric(10, 4), nullable=False),
        sa.Column("index_factor", sa.Numeric(10, 4), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text(NOW_SQL),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("charging_year", name="uq_levy_inflation_index_year"),
        schema="public",
    )
    op.create_index(
        f"ix_public_{INFLATION_INDEX_TABLE}_charging_year",
        INFLATION_INDEX_TABLE,
        ["charging_year"],
        schema="public",
    )

    op.create_table(
        CALCULATIONS_TABLE,
        sa.Column("id", sa.Uuid(), nullable=False),
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
        sa.Column("levy_charge_id", sa.Uuid(), nullable=False),
        sa.Column("edp_start_year_index_id", sa.Uuid(), nullable=True),
        sa.Column("edp_start_year_index_factor", sa.Numeric(10, 4), nullable=True),
        sa.Column("calculation_year_index_id", sa.Uuid(), nullable=True),
        sa.Column("calculation_year_index_factor", sa.Numeric(10, 4), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text(NOW_SQL),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["levy_charge_id"],
            [f"public.{CHARGES_TABLE}.id"],
            name=f"fk_{CALCULATIONS_TABLE}_levy_charge",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["edp_start_year_index_id"],
            [f"public.{INFLATION_INDEX_TABLE}.id"],
            name=f"fk_{CALCULATIONS_TABLE}_edp_start_year_index",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["calculation_year_index_id"],
            [f"public.{INFLATION_INDEX_TABLE}.id"],
            name=f"fk_{CALCULATIONS_TABLE}_calculation_year_index",
            ondelete="RESTRICT",
        ),
        schema="public",
    )
    op.create_index(
        f"ix_public_{CALCULATIONS_TABLE}_quote_reference",
        CALCULATIONS_TABLE,
        ["quote_reference"],
        schema="public",
    )
    op.create_index(
        f"ix_public_{CALCULATIONS_TABLE}_levy_charge_id",
        CALCULATIONS_TABLE,
        ["levy_charge_id"],
        schema="public",
    )


def downgrade() -> None:
    op.execute(f"DROP TABLE IF EXISTS public.{CALCULATIONS_TABLE} CASCADE")
    op.execute(f"DROP TABLE IF EXISTS public.{INFLATION_INDEX_TABLE} CASCADE")
    op.execute(f"DROP TABLE IF EXISTS public.{CHARGES_TABLE} CASCADE")
