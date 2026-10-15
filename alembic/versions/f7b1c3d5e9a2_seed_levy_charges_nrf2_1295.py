"""seed the EDP 1 levy charge when levy_charges is empty (NRF2-1295)

An environment that has never had levy data loaded cannot price a levy, so
POST /levy/recalculate and every quote fail. This inserts the NE-approved
EDP 1 charge, the same row as tests/data/fixtures/levy_charges.csv, but only
into an empty table: an environment whose charges were already loaded keeps
them untouched. load_data.py matches on edp_id + charge_valid_from, so a later
fixture load updates the seeded row in place.

Downgrade is a no-op: a seeded row cannot be told apart from one loaded
later, and audit_levy_calculations rows priced from it block its deletion.

Revision ID: f7b1c3d5e9a2
Revises: e5f9a2b4c6d7
Create Date: 2026-10-14 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "f7b1c3d5e9a2"
down_revision: str | Sequence[str] | None = "e5f9a2b4c6d7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

EDP_NAME = (
    "Norfolk: Broads Special Area of Conservation (SAC) (Yare and Bure), "
    "Broadland Ramsar and River Wensum SAC Environmental Delivery Plan "
    "addressing nutrient pollution (2027 to 2037)"
)


def upgrade() -> None:
    op.execute(
        sa.text(
            "INSERT INTO public.levy_charges"
            " (id, edp_id, edp_name, edp_start_date, charge_valid_from,"
            " charge_valid_to, base_charge_per_unit)"
            " SELECT gen_random_uuid(), 1, :edp_name, DATE '2026-01-01',"
            " DATE '2026-01-01', DATE '2027-12-31', 2675.0000"
            " WHERE NOT EXISTS (SELECT 1 FROM public.levy_charges)"
        ).bindparams(edp_name=EDP_NAME)
    )


def downgrade() -> None:
    # No-op: a seeded row cannot be told apart from one loaded later, and
    # audit_levy_calculations rows priced from it block its deletion.
    pass
