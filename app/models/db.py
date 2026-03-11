"""SQLAlchemy database models for PostGIS reference data.

This module defines the database schema for storing spatial reference data
and lookup tables in PostgreSQL with PostGIS extension.
"""

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from geoalchemy2 import Geometry
from sqlalchemy import (
    DateTime,
    Enum,
    Float,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from app.models.enums import SpatialLayerType


class Base(DeclarativeBase):
    """Base class for all database models."""


class CoefficientLayer(Base):
    """Dedicated model for coefficient polygons (5.4M records)."""

    __tablename__ = "coefficient_layer"
    __table_args__ = {"schema": "nrf_reference"}

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


class SpatialLayer(Base):
    """Unified model for supporting spatial data (catchments, boundaries)."""

    __tablename__ = "spatial_layer"
    __table_args__ = (
        Index("ix_spatial_layer_type_version", "layer_type", "version"),
        {"schema": "nrf_reference"},
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    layer_type: Mapped[SpatialLayerType] = mapped_column(
        Enum(SpatialLayerType, name="spatial_layer_type", schema="nrf_reference"),
        nullable=False,
        index=True,
    )
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

    def __repr__(self) -> str:
        return f"<SpatialLayer(id={self.id}, layer_type={self.layer_type}, name={self.name})>"


class EdpBoundaryLayer(Base):
    """Dedicated model for EDP boundary polygons."""

    __tablename__ = "edp_boundary_layer"
    __table_args__ = {"schema": "nrf_reference"}

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

    def __repr__(self) -> str:
        return f"<EdpBoundaryLayer(id={self.id}, name={self.name})>"


class LookupTable(Base):
    """JSONB-based storage for lookup tables (WwTW, rates)."""

    __tablename__ = "lookup_table"
    __table_args__ = (
        UniqueConstraint("name", "version", name="uq_lookup_name_version"),
        {"schema": "nrf_reference"},
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
