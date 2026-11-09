"""replace levy_charges with edps and base_prices (NRF2-1351)

levy_charges held an EDP's identity and its price in one row per charging
window. They are now two tables: edps holds the EDP (id, name, start_date) and
base_prices holds every base price an EDP has had, each from its own
effective_from. The price in force is the latest row at or before the moment
being priced, so a price change is a new row rather than a new window.

Each EDP's earliest levy_charges row becomes its first base_prices row,
effective from the EDP start date (UK midnight), keeping the row's id so the
audit_levy_calculations.levy_charge_id values that named it still resolve.
Later charging windows are not carried over: they repeated the base price for
the next year, and the year-on-year change is the inflation index's job. When
levy_charges is empty the EDP 1 row and its 2675.00 base price are seeded, as
f7b1c3d5e9a2 did, so an environment can always price a levy.

audit_levy_calculations.levy_charge_id keeps its name and now holds a
base_prices id. Its foreign key is dropped: a base price can be deleted, and
the audit table is being removed in a separate change.

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
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("base_price", sa.Numeric(), nullable=False),
        sa.Column("edp_id", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text(NOW_SQL),
            nullable=False,
        ),
        sa.Column("effective_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("comment", sa.String(), nullable=False),
        sa.Column("user_entra_id", sa.String(), nullable=False),
        sa.Column("user_full_name", sa.String(), nullable=False),
        sa.Column("user_email", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["edp_id"], ["public.edps.id"], name="fk_base_prices_edp"
        ),
        sa.CheckConstraint(
            "base_price > 0 AND base_price < 100000000"
            " AND base_price = round(base_price, 2)",
            name="ck_base_prices_base_price",
        ),
        schema="public",
    )
    op.create_index(
        "ix_public_base_prices_edp_id_effective_from",
        BASE_PRICES_TABLE,
        ["edp_id", "effective_from"],
        schema="public",
    )

    op.execute(
        "INSERT INTO public.edps (id, name, start_date) "
        "SELECT DISTINCT ON (edp_id) edp_id, edp_name, edp_start_date "
        "FROM public.levy_charges ORDER BY edp_id, charge_valid_from"
    )
    op.execute(
        sa.text(
            "INSERT INTO public.base_prices "
            "(id, base_price, edp_id, effective_from, comment, "
            "user_entra_id, user_full_name, user_email) "
            "SELECT DISTINCT ON (edp_id) id, base_charge_per_unit, edp_id, "
            "edp_start_date::timestamp AT TIME ZONE 'Europe/London', "
            ":comment, :entra_id, :full_name, :email "
            "FROM public.levy_charges ORDER BY edp_id, charge_valid_from"
        ).bindparams(
            comment=FIRST_PRICE_COMMENT,
            entra_id=SYSTEM_USER,
            full_name=SYSTEM_USER,
            email=SYSTEM_EMAIL,
        )
    )

    op.execute(
        sa.text(
            "INSERT INTO public.edps (id, name, start_date) "
            "SELECT 1, :edp_name, DATE '2026-01-01' "
            "WHERE NOT EXISTS (SELECT 1 FROM public.edps)"
        ).bindparams(edp_name=EDP_NAME)
    )
    op.execute(
        sa.text(
            "INSERT INTO public.base_prices "
            "(id, base_price, edp_id, effective_from, comment, "
            "user_entra_id, user_full_name, user_email) "
            "SELECT gen_random_uuid(), 2675.00, 1, "
            "DATE '2026-01-01'::timestamp AT TIME ZONE 'Europe/London', "
            ":comment, :entra_id, :full_name, :email "
            "WHERE NOT EXISTS (SELECT 1 FROM public.base_prices)"
        ).bindparams(
            comment=FIRST_PRICE_COMMENT,
            entra_id=SYSTEM_USER,
            full_name=SYSTEM_USER,
            email=SYSTEM_EMAIL,
        )
    )

    op.drop_constraint(
        AUDIT_FK, "audit_levy_calculations", schema="public", type_="foreignkey"
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
    # One open-ended window per EDP at its first base price: the windows the
    # upgrade dropped cannot be rebuilt, and later base prices have no
    # equivalent here.
    op.execute(
        "INSERT INTO public.levy_charges "
        "(id, edp_id, edp_name, edp_start_date, charge_valid_from, "
        "charge_valid_to, base_charge_per_unit) "
        "SELECT DISTINCT ON (e.id) bp.id, e.id, e.name, e.start_date, "
        "e.start_date, DATE '9999-12-31', bp.base_price "
        "FROM public.edps e "
        "JOIN public.base_prices bp ON bp.edp_id = e.id "
        "ORDER BY e.id, bp.effective_from"
    )
    # NOT VALID: audit rows written since the upgrade name base prices that
    # are not levy_charges rows. The constraint still guards new rows.
    op.execute(
        "ALTER TABLE public.audit_levy_calculations "
        f"ADD CONSTRAINT {AUDIT_FK} FOREIGN KEY (levy_charge_id) "
        "REFERENCES public.levy_charges (id) ON DELETE RESTRICT NOT VALID"
    )
    op.drop_table(BASE_PRICES_TABLE, schema="public")
    op.drop_table(EDPS_TABLE, schema="public")
