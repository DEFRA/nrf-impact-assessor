"""replace levy_charges with edps and base_prices (NRF2-1351)

levy_charges held an EDP's identity and its price in one row per charging
window. They are now two tables: edps holds the EDP (id, name, start_date) and
base_prices holds every base price an EDP has had, each from its own
effective_from. The price in force is the latest row at or before the moment
being priced, so a price change is a new row rather than a new window.

levy_charges is dropped, not converted: EDP 1 and its first base price of
2675.00 (effective from the EDP start date, 00:00 UTC) are seeded, as
f7b1c3d5e9a2 did, so an environment can always price a levy.

base_prices.id is an auto-incrementing integer. audit_levy_calculations
.levy_charge_id keeps its name and now holds a base_prices id, so it changes
from uuid to integer, loses its foreign key and becomes nullable. Existing
rows are kept, but the levy_charges ids they held no longer exist and cannot be
converted, so their levy_charge_id becomes NULL. The audit table is being
removed in a separate change.

An EDP has at most one base price per effective_from, so the price in force at
a moment is never ambiguous.

downgrade() keeps only each EDP's first base price, as one open-ended
levy_charges window. Audit rows are kept with a NULL levy_charge_id, which
stays nullable, as the base prices they named are discarded.

Revision ID: b7d3f9a1c5e2
Revises: a8c2e4f6b1d3
Create Date: 2026-11-09 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "b7d3f9a1c5e2"
down_revision: str | Sequence[str] | None = "a8c2e4f6b1d3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

EDPS_TABLE = "edps"
BASE_PRICES_TABLE = "base_prices"
LEVY_CHARGES_TABLE = "levy_charges"
AUDIT_FK = "fk_audit_levy_calculations_levy_charge"
NOW_SQL = "now()"

EDP_NAME = (
    "Norfolk: Broads Special Area of Conservation (SAC) (Yare and Bure), "
    "Broadland Ramsar and River Wensum SAC Environmental Delivery Plan "
    "addressing nutrient pollution (2027 to 2037)"
)
FIRST_PRICE_COMMENT = "Provisional base price when the EDP launched"
SYSTEM_USER = "system"
SYSTEM_EMAIL = "system@nrf.invalid"
AUDIT_TABLE = "audit_levy_calculations"


def upgrade() -> None:
    op.create_table(
        EDPS_TABLE,
        sa.Column("id", sa.Integer(), autoincrement=False, nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        schema="public",
    )
    op.create_table(
        BASE_PRICES_TABLE,
        sa.Column("id", sa.Integer(), sa.Identity(), nullable=False),
        sa.Column("base_price", sa.Numeric(10, 2), nullable=False),
        sa.Column("edp_id", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text(NOW_SQL),
            nullable=False,
        ),
        sa.Column("effective_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("comment", sa.String(500), nullable=False),
        sa.Column("user_entra_id", sa.String(36), nullable=False),
        sa.Column("user_full_name", sa.String(255), nullable=False),
        sa.Column("user_email", sa.String(254), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["edp_id"], ["public.edps.id"], name="fk_base_prices_edp"
        ),
        sa.CheckConstraint(
            "base_price > 0 AND base_price <> 'NaN'",
            name="ck_base_prices_base_price",
        ),
        sa.CheckConstraint(r"comment ~ '\S'", name="ck_base_prices_comment_not_blank"),
        schema="public",
    )
    op.create_index(
        "ix_public_base_prices_edp_id_effective_from",
        BASE_PRICES_TABLE,
        ["edp_id", "effective_from"],
        unique=True,
        schema="public",
    )

    op.execute(
        sa.text(
            "INSERT INTO public.edps (id, name, start_date) "
            "VALUES (1, :edp_name, DATE '2026-01-01')"
        ).bindparams(edp_name=EDP_NAME)
    )
    op.execute(
        sa.text(
            "INSERT INTO public.base_prices "
            "(base_price, edp_id, effective_from, comment, "
            "user_entra_id, user_full_name, user_email) "
            "VALUES (2675.00, 1, TIMESTAMPTZ '2026-01-01 00:00:00+00', "
            ":comment, :entra_id, :full_name, :email)"
        ).bindparams(
            comment=FIRST_PRICE_COMMENT,
            entra_id=SYSTEM_USER,
            full_name=SYSTEM_USER,
            email=SYSTEM_EMAIL,
        )
    )

    op.drop_constraint(AUDIT_FK, AUDIT_TABLE, schema="public", type_="foreignkey")
    op.alter_column(
        AUDIT_TABLE,
        "levy_charge_id",
        schema="public",
        existing_type=sa.Uuid(),
        nullable=True,
    )
    op.alter_column(
        AUDIT_TABLE,
        "levy_charge_id",
        schema="public",
        type_=sa.Integer(),
        existing_type=sa.Uuid(),
        postgresql_using="NULL",
    )
    op.drop_table(LEVY_CHARGES_TABLE, schema="public")


def downgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")
    op.create_table(
        LEVY_CHARGES_TABLE,
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("edp_id", sa.Integer(), nullable=False),
        sa.Column("edp_name", sa.String(), nullable=False),
        sa.Column("edp_start_date", sa.Date(), nullable=False),
        sa.Column("charge_valid_from", sa.Date(), nullable=False),
        sa.Column("charge_valid_to", sa.Date(), nullable=False),
        sa.Column("base_charge_per_unit", sa.Numeric(), nullable=False),
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
        sa.CheckConstraint(
            "base_charge_per_unit > 0 AND base_charge_per_unit < 100000000"
            " AND base_charge_per_unit = round(base_charge_per_unit, 2)",
            name="ck_levy_charges_base_charge_per_unit",
        ),
        schema="public",
    )
    op.execute(
        "ALTER TABLE public.levy_charges "
        "ADD CONSTRAINT ex_levy_charges_edp_window EXCLUDE USING gist "
        "(edp_id WITH =, "
        "daterange(charge_valid_from, charge_valid_to, '[]') WITH &&)"
    )
    op.create_index(
        f"ix_public_{LEVY_CHARGES_TABLE}_edp_id",
        LEVY_CHARGES_TABLE,
        ["edp_id"],
        schema="public",
    )
    # One open-ended window per EDP at its first base price: later base prices
    # have no equivalent here.
    op.execute(
        "INSERT INTO public.levy_charges "
        "(id, edp_id, edp_name, edp_start_date, charge_valid_from, "
        "charge_valid_to, base_charge_per_unit) "
        "SELECT DISTINCT ON (e.id) gen_random_uuid(), e.id, e.name, "
        "e.start_date, e.start_date, DATE '9999-12-31', bp.base_price "
        "FROM public.edps e "
        "JOIN public.base_prices bp ON bp.edp_id = e.id "
        "ORDER BY e.id, bp.effective_from"
    )
    # The audit rows name base prices that the downgrade discards.
    op.alter_column(
        AUDIT_TABLE,
        "levy_charge_id",
        schema="public",
        type_=sa.Uuid(),
        existing_type=sa.Integer(),
        existing_nullable=True,
        postgresql_using="NULL",
    )
    op.create_foreign_key(
        AUDIT_FK,
        AUDIT_TABLE,
        LEVY_CHARGES_TABLE,
        ["levy_charge_id"],
        ["id"],
        source_schema="public",
        referent_schema="public",
        ondelete="RESTRICT",
    )
    op.drop_table(BASE_PRICES_TABLE, schema="public")
    op.drop_table(EDPS_TABLE, schema="public")
