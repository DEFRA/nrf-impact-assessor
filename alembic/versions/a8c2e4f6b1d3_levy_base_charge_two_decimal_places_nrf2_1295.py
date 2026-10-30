"""levy base charge must be positive pounds and pence (NRF2-1295)

The base charge per unit is pounds and pence. ck_levy_charges_base_charge_per_unit
rejects a levy_charges row whose base_charge_per_unit has more than 2 dp, is
zero or negative (which would price a free quote), or is 10^8 or more, so such
a value never reaches the calculation.

PostgreSQL rounds a value to a numeric column's declared scale before a CHECK
runs, so under numeric(12,4) 2675.54999 would be stored as 2675.5500 and pass,
and under numeric(12,2) 2675.555 would be stored as 2675.56. The column becomes
unconstrained numeric (binary-compatible, no table rewrite). The upper bound is
the range numeric(12,4) allowed, so the downgrade back to it cannot overflow.

Adding the constraint fails if an existing row breaks it; that charge has to be
corrected deliberately, not rounded by a migration.

audit_levy_calculations.base_charge_per_unit stays numeric(12,4) with no new
constraint: it copies the value each issued quote was priced from, including
historic 4 dp values such as 2193.6649.

Revision ID: a8c2e4f6b1d3
Revises: e5f9a2b3c4d6
Create Date: 2026-10-29 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "a8c2e4f6b1d3"
down_revision: str | Sequence[str] | None = "e5f9a2b3c4d6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.alter_column(
        "levy_charges",
        "base_charge_per_unit",
        type_=sa.Numeric(),
        existing_type=sa.Numeric(12, 4),
        existing_nullable=False,
        schema="public",
    )
    op.create_check_constraint(
        "ck_levy_charges_base_charge_per_unit",
        "levy_charges",
        "base_charge_per_unit > 0 AND base_charge_per_unit < 100000000"
        " AND base_charge_per_unit = round(base_charge_per_unit, 2)",
        schema="public",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_levy_charges_base_charge_per_unit",
        "levy_charges",
        type_="check",
        schema="public",
    )
    op.alter_column(
        "levy_charges",
        "base_charge_per_unit",
        type_=sa.Numeric(12, 4),
        existing_type=sa.Numeric(),
        existing_nullable=False,
        schema="public",
    )
