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
        CheckConstraint(r"comment ~ '\S'", name="ck_base_prices_comment_not_blank"),
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
    stored. Scales a levy's base charge from the EDP's charging year to a
    later calculation year. The 2026 base row and the 2027 charging-schedule
    row (factor 1.0000) are seeded by migration; later years are added by
    NRF Digital admins via the admin frontend (NRF2-1348), one row per year.
    A row that has priced a quote cannot be deleted (audit FK RESTRICT).
    """

    __tablename__ = "levy_inflation_index"
    __table_args__ = (
        UniqueConstraint("charging_year", name="uq_levy_inflation_index_year"),
        CheckConstraint(
            "cil_index > 0 AND cil_index <> 'NaN'",
            name="ck_levy_inflation_index_cil_index",
        ),
        CheckConstraint(
            r"comment ~ '\S'", name="ck_levy_inflation_index_comment_not_blank"
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

    def __repr__(self) -> str:
        return (
            f"<LevyInflationIndex(year={self.charging_year}, "
            f"factor={self.index_factor})>"
        )


class LevyCalculationRecord(Base):
    """Audit row for one levy calculation.

    One row per successful calculation. A redelivered job that calculates again
    writes another row; the history is the point.

    The values used are copied onto the row, and the *_id columns name the
    base_prices (levy_charge_id) and levy_inflation_index rows they came from.
    Comparing the two against the current row shows whether a value was
    corrected after the event or the wrong row was picked up. The index
    foreign keys are RESTRICT, so an index row that has priced a quote cannot
    be deleted. levy_charge_id has no foreign key because base prices can be
    deleted. The index columns are NULL when no inflation step applied.
    calculator_version pins the rounding rule that derives the amounts from
    base_charge_per_unit, units and the index factors.

    sent_at is set once nrf-backend accepts the quote PATCH priced from this
    row. NULL means delivery was never confirmed: not sent, or sent but the
    stamp itself failed.
    """

    __tablename__ = "audit_levy_calculations"
    __table_args__ = {"schema": "public"}

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    quote_reference: Mapped[str] = mapped_column(String, nullable=False, index=True)
    edp_id: Mapped[int] = mapped_column(Integer, nullable=False)
    edp_name: Mapped[str] = mapped_column(String, nullable=False)
    edp_start_date: Mapped[date] = mapped_column(Date, nullable=False)
    calculator_version: Mapped[int] = mapped_column(Integer, nullable=False)
    base_charge_per_unit: Mapped[Decimal] = mapped_column(
        Numeric(12, 4), nullable=False
    )
    units: Mapped[int] = mapped_column(Integer, nullable=False)
    calculation_date: Mapped[date] = mapped_column(Date, nullable=False)
    provisional_amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    inflation_adjusted_amount: Mapped[Decimal] = mapped_column(
        Numeric(12, 2), nullable=False
    )
    # The base_prices row that priced the quote. Not a foreign key: this table
    # is being removed, and an audit row must outlive an edited or deleted price.
    # NULL on rows recorded before base_prices, whose levy_charges ids are gone.
    levy_charge_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True, index=True
    )
    edp_start_year_index_id: Mapped[UUID | None] = mapped_column(
        ForeignKey(
            "public.levy_inflation_index.id",
            name="fk_audit_levy_calculations_edp_start_year_index",
            ondelete="RESTRICT",
        ),
        nullable=True,
    )
    edp_start_year_index_factor: Mapped[Decimal | None] = mapped_column(
        Numeric(10, 4), nullable=True
    )
    calculation_year_index_id: Mapped[UUID | None] = mapped_column(
        ForeignKey(
            "public.levy_inflation_index.id",
            name="fk_audit_levy_calculations_calculation_year_index",
            ondelete="RESTRICT",
        ),
        nullable=True,
    )
    calculation_year_index_factor: Mapped[Decimal | None] = mapped_column(
        Numeric(10, 4), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    sent_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    def __repr__(self) -> str:
        return (
            f"<LevyCalculationRecord(quote={self.quote_reference}, "
            f"edp_id={self.edp_id}, provisional={self.provisional_amount})>"
        )


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
