"""add levy_charges, levy_calculations and levy_inflation_index

Base charge per unit for each EDP and charging year, used by the assessor to
calculate the provisional nature restoration levy (NRF2-913), the audit
record for every calculation performed (scenario 7): EDP identity and start
date, calculator version, the unrounded and rounded base charge, units,
calculation date, and both totals; and the RICS CIL Index factor per charging
year (relative to the 2026 base, 2026 = 1.0000) used to inflation-adjust a
levy when the calculation date falls in a later charging year than the EDP's
publication.

Three tables squashed into one migration: levy_charges, levy_calculations
and levy_inflation_index were all still unmerged, so carry one revision for
the feature rather than three.

levy_charges is populated here rather than by scripts/load_data.py or data
sync: the table is small, hand-maintained, and not part of any spatial data
drop. No row is seeded by this migration — finance has not yet confirmed a
published base charge or charging year for any EDP, and seeding an unconfirmed
value would let a real quote calculate against it (scenario 5 requires no
assumed/default value). Insert the Norfolk row once finance confirms.

levy_inflation_index IS seeded unconditionally, unlike levy_charges: these
are published RICS figures, not finance-confirmed EDP prices. Seeded with
factors for 2021-2026; later years are added by a follow-up migration once
RICS publishes them.

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
CALCULATIONS_TABLE = "levy_calculations"
INFLATION_INDEX_TABLE = "levy_inflation_index"

# RICS CIL Index factor vs the 2026 base (raw index / raw 2026 index),
# charging year -> factor.
_INDEX_FACTORS = {
    2021: "0.8325",
    2022: "0.8300",
    2023: "0.8875",
    2024: "0.9525",
    2025: "0.9775",
    2026: "1.0000",
}


def seed_index(conn) -> None:
    """Insert the published RICS CIL Index factor rows. Shared with the integration test."""
    for year, factor in _INDEX_FACTORS.items():
        conn.execute(
            sa.text(
                "INSERT INTO public.levy_inflation_index "
                "(id, charging_year, index_factor) VALUES "
                "(gen_random_uuid(), :year, :factor)"
            ),
            {"year": year, "factor": factor},
        )


def upgrade() -> None:
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
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "edp_id", "charge_valid_from", name="uq_levy_charges_edp_from"
        ),
        schema="public",
    )
    op.create_index(
        f"ix_public_{CHARGES_TABLE}_edp_id", CHARGES_TABLE, ["edp_id"], schema="public"
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
        sa.Column("rounded_charge_per_unit", sa.Numeric(12, 2), nullable=False),
        sa.Column("units", sa.Integer(), nullable=False),
        sa.Column("calculation_date", sa.Date(), nullable=False),
        sa.Column("provisional_amount", sa.Numeric(12, 2), nullable=False),
        sa.Column("inflation_adjusted_amount", sa.Numeric(12, 2), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        schema="public",
    )
    op.create_index(
        f"ix_public_{CALCULATIONS_TABLE}_quote_reference",
        CALCULATIONS_TABLE,
        ["quote_reference"],
        schema="public",
    )

    op.create_table(
        INFLATION_INDEX_TABLE,
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("charging_year", sa.Integer(), nullable=False),
        sa.Column("index_factor", sa.Numeric(10, 4), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
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
    seed_index(op.get_bind())


def downgrade() -> None:
    op.execute(f"DROP TABLE IF EXISTS public.{INFLATION_INDEX_TABLE} CASCADE")
    op.execute(f"DROP TABLE IF EXISTS public.{CALCULATIONS_TABLE} CASCADE")
    op.execute(f"DROP TABLE IF EXISTS public.{CHARGES_TABLE} CASCADE")
