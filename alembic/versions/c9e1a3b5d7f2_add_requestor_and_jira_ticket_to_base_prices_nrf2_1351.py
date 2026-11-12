"""add requestor and jira_ticket to base_prices (NRF2-1351)

requestor is the name of the person who asked for the base price change, who
is not necessarily the staff member who saved it. jira_ticket is the NRF2
ticket that records the request.

Both are nullable because rows that already exist (the seeded first price)
have neither. The API requires both for every price it adds.

Revision ID: c9e1a3b5d7f2
Revises: b7d3f9a1c5e2
Create Date: 2026-11-12 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "c9e1a3b5d7f2"
down_revision: str | Sequence[str] | None = "b7d3f9a1c5e2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TABLE = "base_prices"
REQUESTOR_CHECK = "ck_base_prices_requestor_not_blank"
JIRA_TICKET_CHECK = "ck_base_prices_jira_ticket_format"


def upgrade() -> None:
    op.add_column(
        TABLE, sa.Column("requestor", sa.String(255), nullable=True), schema="public"
    )
    op.add_column(
        TABLE, sa.Column("jira_ticket", sa.String(20), nullable=True), schema="public"
    )
    op.create_check_constraint(
        REQUESTOR_CHECK, TABLE, r"requestor ~ '\S'", schema="public"
    )
    op.create_check_constraint(
        JIRA_TICKET_CHECK, TABLE, r"jira_ticket ~ '^NRF2-[0-9]+$'", schema="public"
    )


def downgrade() -> None:
    op.drop_constraint(JIRA_TICKET_CHECK, TABLE, schema="public", type_="check")
    op.drop_constraint(REQUESTOR_CHECK, TABLE, schema="public", type_="check")
    op.drop_column(TABLE, "jira_ticket", schema="public")
    op.drop_column(TABLE, "requestor", schema="public")
