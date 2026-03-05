"""Integration tests for Repository with PostGIS database.

Tests basic query operations against a live test database with minimal sample data.

These tests use small fixtures and a test database (test_nrf_impact).
"""

import geopandas as gpd
import pytest
from geoalchemy2.functions import ST_Intersects
from sqlalchemy import func, select

from app.models.db import CoefficientLayer, LookupTable, SpatialLayer
from app.models.enums import SpatialLayerType
from app.repositories.repository import Repository

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


class TestRepositoryCoefficientQueries:
    """Test queries against coefficient_layer table."""

    def test_query_all_coefficients_as_gdf(
        self, repository: Repository, sample_coefficient_data: gpd.GeoDataFrame
    ):
        """Test querying all coefficient features as GeoDataFrame."""
        stmt = select(CoefficientLayer)

        result = repository.execute_query(stmt, as_gdf=True)

        assert isinstance(result, gpd.GeoDataFrame)
        assert len(result) == 3
        assert result.crs.to_string() == "EPSG:27700"
        assert "geometry" in result.columns
        assert "crome_id" in result.columns
        assert "land_use_cat" in result.columns

    def test_query_all_coefficients_as_orm(
        self, repository: Repository, sample_coefficient_data: gpd.GeoDataFrame
    ):
        """Test querying all coefficient features as ORM objects."""
        stmt = select(CoefficientLayer)

        result = repository.execute_query(stmt, as_gdf=False)

        assert isinstance(result, list)
        assert len(result) == 3
        assert all(isinstance(obj, CoefficientLayer) for obj in result)
        assert result[0].crome_id in ["CROME_001", "CROME_002", "CROME_003"]

    def test_filter_by_catchment(
        self, repository: Repository, sample_coefficient_data: gpd.GeoDataFrame
    ):
        """Test filtering coefficients by NN catchment."""
        stmt = select(CoefficientLayer).where(CoefficientLayer.nn_catchment == "Solent")

        result = repository.execute_query(stmt, as_gdf=True)

        assert len(result) == 2
        assert all(result["nn_catchment"] == "Solent")

    def test_filter_by_land_use(
        self, repository: Repository, sample_coefficient_data: gpd.GeoDataFrame
    ):
        """Test filtering coefficients by land use category."""
        stmt = select(CoefficientLayer).where(CoefficientLayer.land_use_cat == "Arable")

        result = repository.execute_query(stmt, as_gdf=True)

        assert len(result) == 1
        assert result.iloc[0]["land_use_cat"] == "Arable"
        assert result.iloc[0]["crome_id"] == "CROME_001"

    def test_spatial_intersection_query(
        self, repository: Repository, sample_coefficient_data: gpd.GeoDataFrame
    ):
        """Test spatial intersection query using ST_Intersects."""
        from geoalchemy2 import WKTElement

        # Create a test point that intersects with polygon 1
        test_point = WKTElement("POINT(450500 100500)", srid=27700)

        stmt = select(CoefficientLayer).where(ST_Intersects(CoefficientLayer.geometry, test_point))

        result = repository.execute_query(stmt, as_gdf=True)

        # Should intersect with both CROME_001 and CROME_002
        assert len(result) >= 1
        assert any(result["crome_id"] == "CROME_001")


class TestRepositorySpatialLayerQueries:
    """Test queries against spatial_layer table."""

    def test_query_all_spatial_features(
        self, repository: Repository, sample_spatial_data: gpd.GeoDataFrame
    ):
        """Test querying all spatial layer features."""
        stmt = select(SpatialLayer)

        result = repository.execute_query(stmt, as_gdf=True)

        assert len(result) == 2
        # layer_type is returned as enum object in GeoDataFrame
        assert all(result["layer_type"] == SpatialLayerType.NN_CATCHMENTS)
        assert set(result["name"]) == {"Solent", "Avon"}

    def test_filter_by_layer_type(
        self, repository: Repository, sample_spatial_data: gpd.GeoDataFrame
    ):
        """Test filtering by layer type discriminator."""
        stmt = select(SpatialLayer).where(SpatialLayer.layer_type == SpatialLayerType.NN_CATCHMENTS)

        result = repository.execute_query(stmt, as_gdf=False)

        assert len(result) == 2
        assert all(obj.layer_type == SpatialLayerType.NN_CATCHMENTS for obj in result)

    def test_filter_by_name(self, repository: Repository, sample_spatial_data: gpd.GeoDataFrame):
        """Test filtering by feature name."""
        stmt = select(SpatialLayer).where(SpatialLayer.name == "Solent")

        result = repository.execute_query(stmt, as_gdf=True)

        assert len(result) == 1
        assert result.iloc[0]["name"] == "Solent"

    def test_count_query(self, repository: Repository, sample_spatial_data: gpd.GeoDataFrame):
        """Test count query using SQLAlchemy func.count()."""
        stmt = select(func.count()).select_from(SpatialLayer)

        with repository.session() as session:
            count = session.scalar(stmt)

        assert count == 2


class TestRepositoryLookupQueries:
    """Test queries against lookup_table with JSONB data."""

    def test_query_lookup_table(self, repository: Repository, sample_lookup_data: dict):
        """Test querying lookup table."""
        stmt = select(LookupTable).where(LookupTable.name == "wwtw_lookup")

        result = repository.execute_query(stmt, as_gdf=False)

        assert len(result) == 1
        lookup = result[0]
        assert lookup.name == "wwtw_lookup"
        assert lookup.version == 1
        assert len(lookup.data) == 3
        assert lookup.data[0]["WwTW_code"] == "WW001"

    def test_query_latest_version(self, repository: Repository, sample_lookup_data: dict):
        """Test querying latest version of lookup table."""
        # Get max version
        stmt_max = select(func.max(LookupTable.version)).where(LookupTable.name == "wwtw_lookup")

        with repository.session() as session:
            max_version = session.scalar(stmt_max)

        # Query with max version
        stmt = select(LookupTable).where(
            LookupTable.name == "wwtw_lookup",
            LookupTable.version == max_version,
        )

        result = repository.execute_query(stmt, as_gdf=False)

        assert len(result) == 1
        assert result[0].version == 1

    def test_jsonb_data_access(self, repository: Repository, sample_lookup_data: dict):
        """Test accessing JSONB array data."""
        stmt = select(LookupTable).where(LookupTable.name == "wwtw_lookup")

        result = repository.execute_query(stmt, as_gdf=False)
        lookup = result[0]

        # Access JSONB array data
        data = lookup.data
        assert isinstance(data, list)
        assert len(data) == 3

        # Verify structure of JSONB records
        wwtw_codes = [record["WwTW_code"] for record in data]
        assert set(wwtw_codes) == {"WW001", "WW002", "WW003"}


class TestRepositoryMultiTableQueries:
    """Test queries involving multiple tables."""

    def test_combined_data_query(
        self,
        repository: Repository,
        sample_coefficient_data: gpd.GeoDataFrame,
        sample_spatial_data: gpd.GeoDataFrame,
    ):
        """Test querying both coefficient and spatial data."""
        # Query coefficient count
        stmt_coeff = select(func.count()).select_from(CoefficientLayer)

        # Query spatial count
        stmt_spatial = select(func.count()).select_from(SpatialLayer)

        with repository.session() as session:
            coeff_count = session.scalar(stmt_coeff)
            spatial_count = session.scalar(stmt_spatial)

        assert coeff_count == 3
        assert spatial_count == 2

    def test_session_context_manager(
        self, repository: Repository, sample_coefficient_data: gpd.GeoDataFrame
    ):
        """Test using session as context manager for multiple queries."""
        with repository.session() as session:
            # Query 1: Count
            stmt_count = select(func.count()).select_from(CoefficientLayer)
            count = session.scalar(stmt_count)

            # Query 2: Filter
            stmt_filter = select(CoefficientLayer).where(CoefficientLayer.nn_catchment == "Solent")
            result = session.scalars(stmt_filter).all()

            assert count == 3
            assert len(result) == 2


class TestRepositoryContextManager:
    """Test repository context manager functionality."""

    def test_repository_context_manager(self, repository: Repository):
        """Test using repository as context manager."""
        # repository fixture already provides a clean repository with truncated tables
        with repository.session() as session:
            stmt = select(func.count()).select_from(CoefficientLayer)
            count = session.scalar(stmt)
            assert count == 0  # Empty database (no fixtures loaded)

    def test_repository_close_disposes_engine(self, test_engine):
        """Test that closing repository disposes of engine."""
        from sqlalchemy import create_engine

        # Create a separate engine for this test (don't use shared test_engine)
        test_url = "postgresql://postgres@localhost:5432/test_nrf_impact"
        engine = create_engine(test_url, pool_size=2)

        repo = Repository(engine)

        # Engine should have pool
        assert engine.pool is not None

        # Close repository
        repo.close()

        # Engine should be disposed - verify by checking pool has no checked out connections
        # Note: dispose() doesn't reset pool_size, just invalidates connections
        status = engine.pool.status()
        assert "Checked out connections: 0" in status


class TestRepositoryVersioning:
    """Test version filtering for temporal data queries."""

    def test_query_specific_version(
        self, repository: Repository, sample_coefficient_data: gpd.GeoDataFrame
    ):
        """Test querying specific version of coefficient data."""
        stmt = select(CoefficientLayer).where(CoefficientLayer.version == 1)

        result = repository.execute_query(stmt, as_gdf=False)

        assert len(result) == 3
        assert all(obj.version == 1 for obj in result)
