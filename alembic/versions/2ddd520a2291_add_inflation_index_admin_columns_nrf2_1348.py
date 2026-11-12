"""add inflation index admin columns (NRF2-1348)

levy_inflation_index gains the same provenance columns base_prices has:
comment, staff_id, staff_full_name and staff_email. From NRF2-1348 the
table is maintained by NRF Digital admins through the admin frontend, so
each row must record who saved it, when and why. A cil_index check
constraint is added alongside, mirroring ck_base_prices_base_price.

Existing rows (local environments loaded from the dev CSV) are backfilled
with the system staff used by b7d3f9a1c5e2's seed.

The 2026 base row and the 2027 charging-schedule row are seeded
idempotently (ON CONFLICT DO NOTHING). Migrations are the only seeding
path in production — scripts/load_data.py and its CSV fixture are local
dev tools. Both are needed before 1 January 2027: from then every quote
crosses charging years (EDP 1 starts in 2026) and calculate_levy raises
LevyChargeUnavailableError — the job ages to the DLQ and no quote email
is sent — when either the start-year or calculation-year index row is
missing. The 2026 factor is 1.0000 by definition (the base every factor
is measured from); the 2027 factor is 1.0000 because inflation for the
first year is built into the charging schedule, not derived from a
published index.

downgrade() removes only rows still carrying the seed signature
(system staff), so admin-created rows survive, then drops the constraints
and columns.

Revision ID: 2ddd520a2291
Revises: b7d3f9a1c5e2
Create Date: 2026-11-12 14:59:09.778088
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy import bindparam

from alembic import op

revision: str = "2ddd520a2291"
down_revision: str | Sequence[str] | None = "b7d3f9a1c5e2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

INFLATION_INDEX_TABLE = "levy_inflation_index"
CIL_INDEX_CHECK = "ck_levy_inflation_index_cil_index"
COMMENT_CHECK = "ck_levy_inflation_index_comment_not_blank"

SYSTEM_USER = "system"
SYSTEM_EMAIL = "system@nrf.invalid"

# column name -> (length, backfill value). The lengths mirror base_prices.
PROVENANCE_COLUMNS: dict[str, tuple[int, str]] = {
    "comment": (500, "Seeded with the published RICS CIL Index values"),
    "staff_id": (36, SYSTEM_USER),
    "staff_full_name": (255, SYSTEM_USER),
    "staff_email": (254, SYSTEM_EMAIL),
}
BACKFILL_COMMENT = PROVENANCE_COLUMNS["comment"][1]
# charging_year, cil_index, index_factor, comment
SEED_ROWS = (
    (2026, "400.0000", "1.0000", BACKFILL_COMMENT),
    (
        2027,
        "400.0000",
        "1.0000",
        "Factor is 1.0000 throughout 2027, as set by the charging schedule",
    ),
)
SEED_COMMENTS = tuple({comment for _, _, _, comment in SEED_ROWS})


def upgrade() -> None:
    for column, (length, _) in PROVENANCE_COLUMNS.items():
        op.add_column(
            INFLATION_INDEX_TABLE,
            sa.Column(column, sa.String(length), nullable=True),
            schema="public",
        )
    op.execute(
        sa.text(
            f"UPDATE public.{INFLATION_INDEX_TABLE} "  # noqa: S608
            "SET comment = :comment, staff_id = :staff_id, "
            "staff_full_name = :staff_full_name, staff_email = :staff_email"
        ).bindparams(
            comment=PROVENANCE_COLUMNS["comment"][1],
            staff_id=SYSTEM_USER,
            staff_full_name=SYSTEM_USER,
            staff_email=SYSTEM_EMAIL,
        )
    )
    for column, (length, _) in PROVENANCE_COLUMNS.items():
        op.alter_column(
            INFLATION_INDEX_TABLE,
            column,
            existing_type=sa.String(length),
            nullable=False,
            schema="public",
        )
    op.create_check_constraint(
        CIL_INDEX_CHECK,
        INFLATION_INDEX_TABLE,
        "cil_index > 0 AND cil_index <> 'NaN'",
        schema="public",
    )
    op.create_check_constraint(
        COMMENT_CHECK,
        INFLATION_INDEX_TABLE,
        r"comment ~ '\S'",
        schema="public",
    )
    for charging_year, cil_index, index_factor, comment in SEED_ROWS:
        op.execute(
            sa.text(
                "INSERT INTO public.levy_inflation_index "
                "(id, charging_year, cil_index, index_factor, comment, "
                "staff_id, staff_full_name, staff_email) VALUES "
                "(gen_random_uuid(), :year, CAST(:cil_index AS numeric(10, 4)), "
                "CAST(:index_factor AS numeric(10, 4)), :comment, :staff_id, "
                ":staff_full_name, :staff_email) "
                "ON CONFLICT (charging_year) DO NOTHING"
            ).bindparams(
                year=charging_year,
                cil_index=cil_index,
                index_factor=index_factor,
                comment=comment,
                staff_id=SYSTEM_USER,
                staff_full_name=SYSTEM_USER,
                staff_email=SYSTEM_EMAIL,
            )
        )


def downgrade() -> None:
    # Only the migration's own seeds go; rows an admin saved are kept, and
    # any that priced a quote would block here on the audit table's RESTRICT
    # foreign keys, which is the safe direction to fail.
    op.execute(
        sa.text(
            "DELETE FROM public.levy_inflation_index "
            "WHERE staff_id = :staff_id AND comment IN :comments"
        ).bindparams(
            bindparam("comments", value=SEED_COMMENTS, expanding=True),
            staff_id=SYSTEM_USER,
        )
    )
    op.drop_constraint(COMMENT_CHECK, INFLATION_INDEX_TABLE, schema="public")
    op.drop_constraint(CIL_INDEX_CHECK, INFLATION_INDEX_TABLE, schema="public")
    for column in PROVENANCE_COLUMNS:
        op.drop_column(INFLATION_INDEX_TABLE, column, schema="public")
