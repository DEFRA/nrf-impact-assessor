"""SQLAlchemy database models for PostGIS reference data."""

from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from geoalchemy2 import Geometry
from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Identity,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base class for all database models."""


# The shared check expression for every table whose comment must contain a
# non-whitespace character
COMMENT_NOT_BLANK = r"comment ~ '\S'"


class SpatialLayerMixin:
    """Shared columns for spatial layers with name and JSONB attributes."""

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1, index=True)

    geometry: Mapped[Any] = mapped_column(
        Geometry(geometry_type="GEOMETRY", srid=27700, spatial_index=True),
        nullable=False,
    )

    name: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    attributes: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class CoefficientLayer(Base):
    """Dedicated model for coefficient polygons (~31.4M records)."""

    __tablename__ = "coefficient_layer"
    __table_args__ = {"schema": "public"}

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1, index=True)

    geometry: Mapped[Any] = mapped_column(
        Geometry(geometry_type="MULTIPOLYGON", srid=27700, spatial_index=True),
        nullable=False,
    )

    crome_id: Mapped[str | None] = mapped_column(String, nullable=True, index=True)

    lu_curr_n_coeff: Mapped[float | None] = mapped_column(Float, nullable=True)
    lu_curr_p_coeff: Mapped[float | None] = mapped_column(Float, nullable=True)
    n_resi_coeff: Mapped[float | None] = mapped_column(Float, nullable=True)
    p_resi_coeff: Mapped[float | None] = mapped_column(Float, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    def __repr__(self) -> str:
        return f"<CoefficientLayer(id={self.id}, crome_id={self.crome_id})>"


# ---------------------------------------------------------------------------
# Nutrient mitigation layers
# ---------------------------------------------------------------------------


class WwtwCatchments(SpatialLayerMixin, Base):
    """WwTW (wastewater treatment works) catchment polygons."""

    __tablename__ = "wwtw_catchments"
    __table_args__ = {"schema": "public"}

    def __repr__(self) -> str:
        return f"<WwtwCatchments(id={self.id}, name={self.name})>"


class LpaBoundaries(SpatialLayerMixin, Base):
    """Local planning authority boundary polygons."""

    __tablename__ = "lpa_boundaries"
    __table_args__ = {"schema": "public"}

    def __repr__(self) -> str:
        return f"<LpaBoundaries(id={self.id}, name={self.name})>"


class NnCatchments(SpatialLayerMixin, Base):
    """Nutrient neutrality catchment polygons."""

    __tablename__ = "nn_catchments"
    __table_args__ = {"schema": "public"}

    def __repr__(self) -> str:
        return f"<NnCatchments(id={self.id}, name={self.name})>"


class Subcatchments(SpatialLayerMixin, Base):
    """Sub-catchment polygons."""

    __tablename__ = "subcatchments"
    __table_args__ = {"schema": "public"}

    def __repr__(self) -> str:
        return f"<Subcatchments(id={self.id}, name={self.name})>"


# ---------------------------------------------------------------------------
# GCN assessment layers
# ---------------------------------------------------------------------------


class GcnRiskZones(SpatialLayerMixin, Base):
    """GCN (great crested newt) risk zone polygons (red/amber/green)."""

    __tablename__ = "gcn_risk_zones"
    __table_args__ = {"schema": "public"}

    def __repr__(self) -> str:
        return f"<GcnRiskZones(id={self.id}, name={self.name})>"


class GcnPonds(SpatialLayerMixin, Base):
    """National ponds dataset used for GCN assessment."""

    __tablename__ = "gcn_ponds"
    __table_args__ = {"schema": "public"}

    def __repr__(self) -> str:
        return f"<GcnPonds(id={self.id}, name={self.name})>"


class EdpEdges(SpatialLayerMixin, Base):
    """Environmental designation polygon edges used in GCN assessment."""

    __tablename__ = "edp_edges"
    __table_args__ = {"schema": "public"}

    def __repr__(self) -> str:
        return f"<EdpEdges(id={self.id}, name={self.name})>"


class EdpBoundaryLayer(SpatialLayerMixin, Base):
    """Dedicated model for EDP boundary polygons."""

    __tablename__ = "edp_boundary_layer"
    __table_args__ = {"schema": "public"}

    def __repr__(self) -> str:
        return f"<EdpBoundaryLayer(id={self.id}, name={self.name})>"


class EdpExcludedAreas(SpatialLayerMixin, Base):
    """Buffered SSSI exclusion-area polygons (nutrient EDP)."""

    __tablename__ = "edp_excluded_areas"
    __table_args__ = {"schema": "public"}

    def __repr__(self) -> str:
        return f"<EdpExcludedAreas(id={self.id}, name={self.name})>"


class LookupTable(Base):
    """JSONB-based storage for lookup tables (WwTW, rates)."""

    __tablename__ = "lookup_table"
    __table_args__ = (
        UniqueConstraint("name", "version", name="uq_lookup_name_version"),
        {"schema": "public"},
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(String, nullable=False, index=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1, index=True)

    data: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    schema: Mapped[dict[str, str] | None] = mapped_column(JSONB, nullable=True)

    description: Mapped[str | None] = mapped_column(String, nullable=True)
    source: Mapped[str | None] = mapped_column(String, nullable=True)
    license: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    def __repr__(self) -> str:
        return f"<LookupTable(id={self.id}, name={self.name}, rows={len(self.data)})>"


class Edp(Base):
    """An Environmental Delivery Plan the levy is charged for.

    Seeded by migration, not by data sync. The id is the EDP_Id the boundary
    layer carries, so it is assigned rather than generated.
    """

    __tablename__ = "edps"
    __table_args__ = {"schema": "public"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    start_date: Mapped[date] = mapped_column(Date, nullable=False)

    def __repr__(self) -> str:
        return f"<Edp(id={self.id}, name={self.name})>"


class BasePrice(Base):
    """Base price per unit for an EDP from effective_from onwards.

    The price in force at an instant is the EDP's row with the latest
    effective_from at or before it. Each price change is its own row; the first
    row is the provisional price the EDP launched with.
    """

    __tablename__ = "base_prices"
    __table_args__ = (
        CheckConstraint(
            "base_price > 0 AND base_price <> 'NaN'",
            name="ck_base_prices_base_price",
        ),
        CheckConstraint(COMMENT_NOT_BLANK, name="ck_base_prices_comment_not_blank"),
        CheckConstraint(r"requestor ~ '\S'", name="ck_base_prices_requestor_not_blank"),
        CheckConstraint(
            r"jira_ticket ~ '^NRF2-[0-9]+$'", name="ck_base_prices_jira_ticket_format"
        ),
        Index(
            "ix_public_base_prices_edp_id_effective_from",
            "edp_id",
            "effective_from",
            unique=True,
        ),
        {"schema": "public"},
    )

    id: Mapped[int] = mapped_column(Integer, Identity(), primary_key=True)
    # Pounds and pence up to 99999999.99. numeric(10, 2) rounds a 3+ dp value
    # rather than rejecting it, so callers must validate 2 dp before inserting;
    # ck_base_prices_base_price keeps the price positive and not NaN.
    base_price: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    edp_id: Mapped[int] = mapped_column(
        ForeignKey("public.edps.id", name="fk_base_prices_edp"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    effective_from: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    comment: Mapped[str] = mapped_column(String(500), nullable=False)
    # The identity provider's id for the staff member (the Entra oid claim today),
    # not an id from the backend users table.
    staff_id: Mapped[str] = mapped_column(String(36), nullable=False)
    staff_full_name: Mapped[str] = mapped_column(String(255), nullable=False)
    staff_email: Mapped[str] = mapped_column(String(254), nullable=False)
    # Who asked for the change, and the ticket recording the request. Null only
    # on rows that predate these columns.
    requestor: Mapped[str | None] = mapped_column(String(255), nullable=True)
    jira_ticket: Mapped[str | None] = mapped_column(String(20), nullable=True)

    def __repr__(self) -> str:
        return (
            f"<BasePrice(edp_id={self.edp_id}, from={self.effective_from}, "
            f"price={self.base_price})>"
        )


class LevyInflationIndex(Base):
    """RICS CIL Index for one charging year, relative to the fixed 2026 base
    (2026 = 1.0000).

    The index is in force for the whole of its charging year: the effective
    date is 1 January 00:00 UTC of charging_year, so it is derived, not
    stored. The levy scales by cil_index from the base price's effective year
    to the calculation year. The 2026 base row and the 2027 charging-schedule
    row (factor 1.0000) are seeded by migration; later years are added by
    NRF Digital admins via the admin frontend (NRF2-1348), one row per year.
    """

    __tablename__ = "levy_inflation_index"
    __table_args__ = (
        UniqueConstraint("charging_year", name="uq_levy_inflation_index_year"),
        CheckConstraint(
            "cil_index > 0 AND cil_index <> 'NaN'",
            name="ck_levy_inflation_index_cil_index",
        ),
        CheckConstraint(
            COMMENT_NOT_BLANK, name="ck_levy_inflation_index_comment_not_blank"
        ),
        CheckConstraint(
            r"requestor ~ '\S'", name="ck_levy_inflation_index_requestor_not_blank"
        ),
        CheckConstraint(
            r"jira_ticket ~ '^NRF2-[0-9]+$'",
            name="ck_levy_inflation_index_jira_ticket_format",
        ),
        {"schema": "public"},
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    charging_year: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    # The published RICS CIL Index number for the charging year. The factor
    # applied to a levy is cil_index / the 2026 base row's cil_index, stored
    # in index_factor; ck_levy_inflation_index_cil_index keeps it positive and
    # not NaN.
    cil_index: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    index_factor: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    comment: Mapped[str] = mapped_column(String(500), nullable=False)
    # The identity provider's id for the staff member (the Entra oid claim today),
    # not an id from the backend users table.
    staff_id: Mapped[str] = mapped_column(String(36), nullable=False)
    staff_full_name: Mapped[str] = mapped_column(String(255), nullable=False)
    staff_email: Mapped[str] = mapped_column(String(254), nullable=False)
    # Who asked for the change, and the ticket recording the request. Null only
    # on rows that predate these columns.
    requestor: Mapped[str | None] = mapped_column(String(255), nullable=True)
    jira_ticket: Mapped[str | None] = mapped_column(String(20), nullable=True)

    def __repr__(self) -> str:
        return (
            f"<LevyInflationIndex(year={self.charging_year}, "
            f"factor={self.index_factor})>"
        )


class LevyInflationIndexHistory(Base):
    """History row for one inflation index addition.

    Copies the index's values as it was added, so a history row outlives the
    amendment and the deletion of its index. The row's staff_* is the admin
    who added it; comment is the comment they entered. Rows before this table
    existed (the seeds) have no history; the history starts when the table
    does.
    """

    __tablename__ = "levy_inflation_index_history"
    __table_args__ = (
        CheckConstraint(
            COMMENT_NOT_BLANK,
            name="ck_levy_inflation_index_history_comment_not_blank",
        ),
        {"schema": "public"},
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    # Names the index row this addition was about. No foreign key: a deleted
    # index must not take its history with it (the calculation audit's RESTRICT
    # links still stop an index that priced a quote being deleted at all).
    inflation_index_id: Mapped[UUID] = mapped_column(nullable=False, index=True)
    charging_year: Mapped[int] = mapped_column(Integer, nullable=False)
    # The index's values as it was added. index_factor is copied so the
    # history shows the factor the addition produced, even after a later
    # amendment re-derives the live row's.
    cil_index: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    index_factor: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    comment: Mapped[str] = mapped_column(String(500), nullable=False)
    staff_id: Mapped[str] = mapped_column(String(36), nullable=False)
    staff_full_name: Mapped[str] = mapped_column(String(255), nullable=False)
    staff_email: Mapped[str] = mapped_column(String(254), nullable=False)
    requestor: Mapped[str | None] = mapped_column(String(255), nullable=True)
    jira_ticket: Mapped[str | None] = mapped_column(String(20), nullable=True)
    # When the index was added. Ordered descending, this is the additions
    # history the admin frontend displays.
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    def __repr__(self) -> str:
        return f"<LevyInflationIndexHistory(year={self.charging_year})>"


class DataSyncRun(Base):
    """Async reload job record. Status: 'running' | 'success' | 'failed'."""

    __tablename__ = "data_sync_run"
    __table_args__ = (
        Index(
            "uq_data_sync_run_single_running",
            "status",
            unique=True,
            postgresql_where=text("status = 'running'"),
        ),
        {"schema": "public"},
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    status: Mapped[str] = mapped_column(String, nullable=False)
    data_version: Mapped[str | None] = mapped_column(String, nullable=True)
    forced: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    error: Mapped[str | None] = mapped_column(String, nullable=True)


class DataLoadHistory(Base):
    """Per-table audit row for a reload run."""

    __tablename__ = "data_load_history"
    __table_args__ = {"schema": "public"}

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    run_id: Mapped[UUID] = mapped_column(
        ForeignKey("public.data_sync_run.id"), nullable=False
    )
    table_name: Mapped[str] = mapped_column(String, nullable=False)
    s3_key: Mapped[str] = mapped_column(String, nullable=False)
    etag: Mapped[str] = mapped_column(String, nullable=False)
    data_version: Mapped[str | None] = mapped_column(String, nullable=True)
    row_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    status: Mapped[str] = mapped_column(String, nullable=False)
    status_detail: Mapped[str | None] = mapped_column(String, nullable=True)
    loaded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class DataActiveVersion(Base):
    """Points at the version of each reference table that reads should use.

    Reads fall back to MAX(version) when no row exists here (see
    app/data_sync/active_version.py), so this table only needs a row once a
    reload or rollback has actually run.
    """

    __tablename__ = "data_active_version"
    __table_args__ = {"schema": "public"}

    table_name: Mapped[str] = mapped_column(String, primary_key=True)
    active_version: Mapped[int] = mapped_column(Integer, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class DataRollbackEvent(Base):
    """Audit row for one table's active-version rollback."""

    __tablename__ = "data_rollback_event"
    __table_args__ = {"schema": "public"}

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    table_name: Mapped[str] = mapped_column(String, nullable=False)
    from_version: Mapped[int] = mapped_column(Integer, nullable=False)
    to_version: Mapped[int] = mapped_column(Integer, nullable=False)
    rolled_back_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class ModelResultRecord(Base):
    """Audit row for one assessment run: its output, or why it failed.

    status is 'success' | 'failed'. One row per process_job attempt, so a
    redelivered job records one row per try.
    """

    __tablename__ = "audit_model_results"
    __table_args__ = {"schema": "public"}

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    status: Mapped[str] = mapped_column(String, nullable=False)
    nrl_reference: Mapped[str] = mapped_column(String, nullable=False, index=True)
    assessment_type: Mapped[str] = mapped_column(String, nullable=False)
    model_version: Mapped[str] = mapped_column(String, nullable=False)
    model_output: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    error_details: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
