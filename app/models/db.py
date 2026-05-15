"""SQLAlchemy database models for PostGIS reference data."""

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from geoalchemy2 import Geometry
from sqlalchemy import DateTime, Float, Integer, String, UniqueConstraint, func
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
    """Dedicated model for coefficient polygons (5.4M records)."""

    __tablename__ = "coefficient_layer"
    __table_args__ = {"schema": "public"}

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1, index=True)

    geometry: Mapped[Any] = mapped_column(
        Geometry(geometry_type="MULTIPOLYGON", srid=27700, spatial_index=True),
        nullable=False,
    )

    crome_id: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    land_use_cat: Mapped[str | None] = mapped_column(String, nullable=True)
    nn_catchment: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    subcatchment: Mapped[str | None] = mapped_column(String, nullable=True, index=True)

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
