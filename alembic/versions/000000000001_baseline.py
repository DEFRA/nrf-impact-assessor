"""baseline

Squashed from:
  6a7baea82d03  initial version
  45ce553d0710  add GCN spatial layer types
  1bf027d04bb3  add EDP boundary layer table
  3f8c21a9e047  add performance indexes

Revision ID: 000000000001
Revises:
Create Date: 2026-05-15 00:00:00.000000
"""

from collections.abc import Sequence

import geoalchemy2
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "000000000001"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

NOW = sa.text("now()")


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS nrf_reference")
    op.execute("CREATE EXTENSION IF NOT EXISTS postgis")

    sa.Enum(
        "WWTW_CATCHMENTS",
        "LPA_BOUNDARIES",
        "NN_CATCHMENTS",
        "SUBCATCHMENTS",
        "GCN_RISK_ZONES",
        "GCN_PONDS",
        "EDP_EDGES",
        name="spatial_layer_type",
        schema="nrf_reference",
    ).create(op.get_bind())

    op.create_table(
        "coefficient_layer",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column(
            "geometry",
            geoalchemy2.types.Geometry(
                geometry_type="MULTIPOLYGON",
                srid=27700,
                dimension=2,
                from_text="ST_GeomFromEWKT",
                name="geometry",
                nullable=False,
            ),
            nullable=False,
        ),
        sa.Column("crome_id", sa.String(), nullable=True),
        sa.Column("land_use_cat", sa.String(), nullable=True),
        sa.Column("nn_catchment", sa.String(), nullable=True),
        sa.Column("subcatchment", sa.String(), nullable=True),
        sa.Column("lu_curr_n_coeff", sa.Float(), nullable=True),
        sa.Column("lu_curr_p_coeff", sa.Float(), nullable=True),
        sa.Column("n_resi_coeff", sa.Float(), nullable=True),
        sa.Column("p_resi_coeff", sa.Float(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=NOW, nullable=False
        ),
        sa.PrimaryKeyConstraint("id"),
        schema="nrf_reference",
    )
    op.create_index(
        "ix_nrf_reference_coefficient_layer_crome_id",
        "coefficient_layer",
        ["crome_id"],
        schema="nrf_reference",
    )
    op.create_index(
        "ix_nrf_reference_coefficient_layer_nn_catchment",
        "coefficient_layer",
        ["nn_catchment"],
        schema="nrf_reference",
    )
    op.create_index(
        "ix_nrf_reference_coefficient_layer_subcatchment",
        "coefficient_layer",
        ["subcatchment"],
        schema="nrf_reference",
    )
    op.create_index(
        "ix_nrf_reference_coefficient_layer_version",
        "coefficient_layer",
        ["version"],
        schema="nrf_reference",
    )
    # Full GiST index created automatically by GeoAlchemy2 (spatial_index=True default)
    # Partial GiST covering only version 1 — add an equivalent when loading a new version
    op.execute(
        sa.text(
            "CREATE INDEX ix_nrf_reference_coefficient_layer_geom_v1 "
            "ON nrf_reference.coefficient_layer USING GIST (geometry) WHERE version = 1"
        )
    )

    op.create_table(
        "lookup_table",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("data", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("schema", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("description", sa.String(), nullable=True),
        sa.Column("source", sa.String(), nullable=True),
        sa.Column("license", sa.String(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=NOW, nullable=False
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", "version", name="uq_lookup_name_version"),
        schema="nrf_reference",
    )
    op.create_index(
        "ix_nrf_reference_lookup_table_name",
        "lookup_table",
        ["name"],
        schema="nrf_reference",
    )
    op.create_index(
        "ix_nrf_reference_lookup_table_version",
        "lookup_table",
        ["version"],
        schema="nrf_reference",
    )

    op.create_table(
        "spatial_layer",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "layer_type",
            postgresql.ENUM(
                "WWTW_CATCHMENTS",
                "LPA_BOUNDARIES",
                "NN_CATCHMENTS",
                "SUBCATCHMENTS",
                "GCN_RISK_ZONES",
                "GCN_PONDS",
                "EDP_EDGES",
                name="spatial_layer_type",
                schema="nrf_reference",
                create_type=False,
            ),
            nullable=False,
        ),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column(
            "geometry",
            geoalchemy2.types.Geometry(
                srid=27700,
                dimension=2,
                from_text="ST_GeomFromEWKT",
                name="geometry",
                nullable=False,
            ),
            nullable=False,
        ),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("attributes", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=NOW, nullable=False
        ),
        sa.PrimaryKeyConstraint("id"),
        schema="nrf_reference",
    )
    op.create_index(
        "ix_nrf_reference_spatial_layer_layer_type",
        "spatial_layer",
        ["layer_type"],
        schema="nrf_reference",
    )
    op.create_index(
        "ix_nrf_reference_spatial_layer_name",
        "spatial_layer",
        ["name"],
        schema="nrf_reference",
    )
    op.create_index(
        "ix_nrf_reference_spatial_layer_version",
        "spatial_layer",
        ["version"],
        schema="nrf_reference",
    )
    # Full GiST index created automatically by GeoAlchemy2 (spatial_index=True default)
    op.create_index(
        "ix_spatial_layer_type_version",
        "spatial_layer",
        ["layer_type", "version"],
        schema="nrf_reference",
    )

    op.create_table(
        "edp_boundary_layer",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column(
            "geometry",
            geoalchemy2.types.Geometry(
                geometry_type="GEOMETRY",
                srid=27700,
                spatial_index=False,
            ),
            nullable=False,
        ),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("attributes", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=NOW, nullable=False
        ),
        sa.PrimaryKeyConstraint("id"),
        schema="nrf_reference",
    )
    op.create_index(
        "ix_nrf_reference_edp_boundary_layer_name",
        "edp_boundary_layer",
        ["name"],
        schema="nrf_reference",
    )
    op.create_index(
        "ix_nrf_reference_edp_boundary_layer_version",
        "edp_boundary_layer",
        ["version"],
        schema="nrf_reference",
    )
    op.execute(
        sa.text(
            "CREATE INDEX ix_nrf_reference_edp_boundary_layer_geometry "
            "ON nrf_reference.edp_boundary_layer USING GIST (geometry)"
        )
    )


def downgrade() -> None:
    op.execute("DROP SCHEMA IF EXISTS nrf_reference CASCADE")
