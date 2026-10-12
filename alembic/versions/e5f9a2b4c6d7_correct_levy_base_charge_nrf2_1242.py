"""correct the EDP 1 base charge to the NE-approved 2675 (NRF2-1242)

The row was entered from NE's Levy Liability Calculator v0.2; the newer
calculator's EDP Register holds the approved price, 2675.0000 per unit
excluding VAT. An in-place correction, not a new charging window: the old
value was wrong when it was entered, and audit_levy_calculations copies the
values each quote was priced from, so quotes already issued at 2193.6649
stay explainable.

The UPDATE is keyed on edp_id + charge_valid_from because the row's id UUID
was generated per environment. It no-ops on a fresh database (migrations run
before fixtures load the row) and raises only when the table holds rows but
not the expected one, so an environment whose data has drifted fails the
migration instead of silently continuing to quote the wrong price.

Revision ID: e5f9a2b4c6d7
Revises: d4e8f1a2b3c5
Create Date: 2026-10-08 00:00:00.000000
"""

from collections.abc import Sequence
from decimal import Decimal

import sqlalchemy as sa

from alembic import op

revision: str = "e5f9a2b4c6d7"
down_revision: str | Sequence[str] | None = "d4e8f1a2b3c5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _set_base_charge_per_unit(value: str) -> None:
    """Keyed on edp_id + charge_valid_from (the row's id UUID differs per env).

    No-ops on a fresh database, which runs migrations before fixtures load the
    row; raises only when the table holds rows but not the expected one, so a
    deployed environment whose data has drifted fails the migration instead of
    silently continuing to quote the wrong price.
    """
    op.execute(
        sa.text(
            "DO $$ BEGIN"
            " UPDATE public.levy_charges"
            " SET base_charge_per_unit = :value"
            " WHERE edp_id = 1 AND charge_valid_from = DATE '2026-01-01';"
            " IF NOT FOUND AND EXISTS (SELECT 1 FROM public.levy_charges) THEN"
            " RAISE EXCEPTION"
            " 'no levy_charges row for edp_id=1, charge_valid_from=2026-01-01';"
            " END IF;"
            " END $$;"
        ).bindparams(value=Decimal(value))
    )


def upgrade() -> None:
    _set_base_charge_per_unit("2675.0000")


def downgrade() -> None:
    _set_base_charge_per_unit("2193.6649")
