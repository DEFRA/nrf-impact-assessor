"""add requestor and jira_ticket to levy_inflation_index (NRF2-1348)

requestor is the name of the person who asked for the inflation index change,
who is not necessarily the staff member who saved it. jira_ticket is the NRF2
ticket that records the request. Mirrors c9e1a3b5d7f2 on base_prices, so the
two admin pages record provenance the same way.

Both are nullable because rows that already exist (the seeds and any row an
admin saved before this change) have neither. The API requires both for every
index it adds.

Revision ID: e05a352f9ebe
Revises: 2ddd520a2291
Create Date: 2026-11-16 09:30:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "e05a352f9ebe"
down_revision: str | Sequence[str] | None = "2ddd520a2291"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TABLE = "levy_inflation_index"
REQUESTOR_CHECK = "ck_levy_inflation_index_requestor_not_blank"
JIRA_TICKET_CHECK = "ck_levy_inflation_index_jira_ticket_format"


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
