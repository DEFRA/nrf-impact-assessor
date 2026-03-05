"""Integration test fixtures for PostGIS repository tests."""

import subprocess
from pathlib import Path
from uuid import uuid4

import geopandas as gpd
import pytest
from shapely.geometry import MultiPolygon, Polygon
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from app.models.enums import SpatialLayerType
from app.repositories.repository import Repository

CRS_BNG = "EPSG:27700"


@pytest.fixture(scope="session")
def test_engine() -> Engine:
    """Create test database and return engine.

    This is a session-scoped fixture that:
    1. Creates test_nrf_impact database
    2. Runs migrations (creates schema and tables)
    3. Returns engine for test use
    4. Drops database after all tests complete
    """
    admin_db_url = "postgresql://postgres@localhost:5432/postgres"  # NOSONAR
    test_db_url = "postgresql://postgres@localhost:5432/test_nrf_impact"  # NOSONAR

    # Connect to default postgres database to create test database
    admin_engine = create_engine(admin_db_url)

    # Drop and recreate test database
    with admin_engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        # Terminate existing connections
        conn.execute(
            text(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = 'test_nrf_impact' AND pid <> pg_backend_pid()"
            )
        )
        # Drop if exists
        conn.execute(text("DROP DATABASE IF EXISTS test_nrf_impact"))
        # Create fresh
        conn.execute(text("CREATE DATABASE test_nrf_impact"))

    admin_engine.dispose()

    # Create engine for test database
    engine = create_engine(test_db_url, echo=False)

    # Enable PostGIS extension (required before running migrations)
    with engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))

    # Run Alembic migrations to create schema and tables
    # This ensures tests use the same migration logic as production
    alembic_ini = Path(__file__).parent.parent.parent / "alembic.ini"
    env = {
        "DB_DATABASE": "test_nrf_impact",
        "DB_IAM_AUTHENTICATION": "false",
        "DB_LOCAL_PASSWORD": "",
    }

    cmd = ["alembic", "-c", str(alembic_ini), "upgrade", "head"]
    result = subprocess.run(  # noqa: S603, S607
        cmd,
        env={**subprocess.os.environ, **env},
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode != 0:
        msg = f"Alembic migration failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
        raise RuntimeError(msg)

    yield engine

    # Cleanup: drop test database
    engine.dispose()
    admin_engine = create_engine(admin_db_url)
    with admin_engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(
            text(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = 'test_nrf_impact' AND pid <> pg_backend_pid()"
            )
        )
        conn.execute(text("DROP DATABASE IF EXISTS test_nrf_impact"))
    admin_engine.dispose()


@pytest.fixture
def repository(test_engine: Engine) -> Repository:
    """Create Repository instance with clean database for each test.

    Function-scoped fixture that truncates tables before each test to ensure
    test isolation.
    """
    # Truncate all tables before each test
    with test_engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(text("TRUNCATE nrf_reference.coefficient_layer CASCADE"))
        conn.execute(text("TRUNCATE nrf_reference.spatial_layer CASCADE"))
        conn.execute(text("TRUNCATE nrf_reference.lookup_table CASCADE"))

    return Repository(test_engine)


@pytest.fixture
def sample_coefficient_data(repository: Repository) -> gpd.GeoDataFrame:
    """Load minimal coefficient layer test data.

    Creates 3 sample coefficient polygons covering different land uses
    and catchments for testing spatial queries.
    """
    # Create 3 sample coefficient polygons
    polygons = [
        # Polygon 1: Arable land in Solent catchment
        {
            "id": uuid4(),
            "version": 1,
            "geometry": MultiPolygon(
                [
                    Polygon(
                        [
                            (450000, 100000),
                            (451000, 100000),
                            (451000, 101000),
                            (450000, 101000),
                            (450000, 100000),
                        ]
                    )
                ]
            ),
            "crome_id": "CROME_001",
            "land_use_cat": "Arable",
            "nn_catchment": "Solent",
            "subcatchment": "Test Sub 1",
            "lu_curr_n_coeff": 15.5,
            "lu_curr_p_coeff": 1.2,
            "n_resi_coeff": 5.0,
            "p_resi_coeff": 0.5,
        },
        # Polygon 2: Grassland in Solent catchment (overlapping)
        {
            "id": uuid4(),
            "version": 1,
            "geometry": MultiPolygon(
                [
                    Polygon(
                        [
                            (450500, 100500),
                            (451500, 100500),
                            (451500, 101500),
                            (450500, 101500),
                            (450500, 100500),
                        ]
                    )
                ]
            ),
            "crome_id": "CROME_002",
            "land_use_cat": "Grassland",
            "nn_catchment": "Solent",
            "subcatchment": "Test Sub 1",
            "lu_curr_n_coeff": 8.0,
            "lu_curr_p_coeff": 0.8,
            "n_resi_coeff": 5.0,
            "p_resi_coeff": 0.5,
        },
        # Polygon 3: Woodland in different catchment
        {
            "id": uuid4(),
            "version": 1,
            "geometry": MultiPolygon(
                [
                    Polygon(
                        [
                            (500000, 200000),
                            (501000, 200000),
                            (501000, 201000),
                            (500000, 201000),
                            (500000, 200000),
                        ]
                    )
                ]
            ),
            "crome_id": "CROME_003",
            "land_use_cat": "Woodland",
            "nn_catchment": "Avon",
            "subcatchment": "Test Sub 2",
            "lu_curr_n_coeff": 2.0,
            "lu_curr_p_coeff": 0.1,
            "n_resi_coeff": 5.0,
            "p_resi_coeff": 0.5,
        },
    ]

    gdf = gpd.GeoDataFrame(polygons, crs=CRS_BNG)

    # Load into database using to_postgis
    gdf.to_postgis(
        name="coefficient_layer",
        con=repository.engine,
        schema="nrf_reference",
        if_exists="append",
        index=False,
    )

    return gdf


@pytest.fixture
def sample_spatial_data(repository: Repository) -> gpd.GeoDataFrame:
    """Load minimal spatial layer test data.

    Creates 2 sample spatial features (catchment boundaries) for testing
    layer type filtering and spatial queries.
    """
    # Create 2 sample catchment boundaries
    features = [
        # Feature 1: Solent NN catchment
        {
            "id": uuid4(),
            "layer_type": SpatialLayerType.NN_CATCHMENTS.name,  # Use uppercase name
            "version": 1,
            "geometry": Polygon(
                [
                    (449000, 99000),
                    (452000, 99000),
                    (452000, 102000),
                    (449000, 102000),
                    (449000, 99000),
                ]
            ),
            "name": "Solent",
        },
        # Feature 2: Avon NN catchment
        {
            "id": uuid4(),
            "layer_type": SpatialLayerType.NN_CATCHMENTS.name,  # Use uppercase name
            "version": 1,
            "geometry": Polygon(
                [
                    (499000, 199000),
                    (502000, 199000),
                    (502000, 202000),
                    (499000, 202000),
                    (499000, 199000),
                ]
            ),
            "name": "Avon",
        },
    ]

    gdf = gpd.GeoDataFrame(features, crs=CRS_BNG)

    # Load into database using to_postgis
    gdf.to_postgis(
        name="spatial_layer",
        con=repository.engine,
        schema="nrf_reference",
        if_exists="append",
        index=False,
    )

    return gdf


@pytest.fixture
def sample_lookup_data(repository: Repository) -> dict:
    """Load minimal lookup table test data.

    Creates a small WwTW lookup table for testing JSONB queries.
    """
    from sqlalchemy import insert

    from app.models.db import LookupTable

    lookup_data = {
        "id": uuid4(),
        "name": "wwtw_lookup",
        "version": 1,
        "data": [
            {"WwTW_code": "WW001", "WwTW_name": "Test WwTW 1", "n_removal": 0.3},
            {"WwTW_code": "WW002", "WwTW_name": "Test WwTW 2", "n_removal": 0.4},
            {"WwTW_code": "WW003", "WwTW_name": "Test WwTW 3", "n_removal": 0.5},
        ],
        "description": "Test WwTW lookup data",
    }

    # Insert using SQLAlchemy
    with repository.session() as session:
        stmt = insert(LookupTable).values(**lookup_data)
        session.execute(stmt)
        session.commit()

    return lookup_data


# ======================================================================================
# GCN Assessment Fixtures
# ======================================================================================


@pytest.fixture
def sample_gcn_risk_zones(repository: Repository) -> gpd.GeoDataFrame:
    """Load minimal GCN risk zones test data.

    Creates 3 risk zone polygons (Red, Amber, Green) for testing GCN assessments.
    """
    from shapely.geometry import Polygon
    from sqlalchemy.dialects.postgresql import JSONB

    risk_zones = [
        # Red zone (high risk)
        {
            "id": uuid4(),
            "layer_type": SpatialLayerType.GCN_RISK_ZONES.name,
            "version": 1,
            "geometry": Polygon(
                [
                    (450000, 100000),
                    (450500, 100000),
                    (450500, 100500),
                    (450000, 100500),
                    (450000, 100000),
                ]
            ),
            "attributes": {"RZ": "Red"},
        },
        # Amber zone (medium risk)
        {
            "id": uuid4(),
            "layer_type": SpatialLayerType.GCN_RISK_ZONES.name,
            "version": 1,
            "geometry": Polygon(
                [
                    (450500, 100000),
                    (451000, 100000),
                    (451000, 100500),
                    (450500, 100500),
                    (450500, 100000),
                ]
            ),
            "attributes": {"RZ": "Amber"},
        },
        # Green zone (low risk)
        {
            "id": uuid4(),
            "layer_type": SpatialLayerType.GCN_RISK_ZONES.name,
            "version": 1,
            "geometry": Polygon(
                [
                    (450000, 100500),
                    (451000, 100500),
                    (451000, 101000),
                    (450000, 101000),
                    (450000, 100500),
                ]
            ),
            "attributes": {"RZ": "Green"},
        },
    ]

    gdf = gpd.GeoDataFrame(risk_zones, crs=CRS_BNG)

    # Convert attributes dict to JSON string for JSONB column
    import json

    gdf["attributes"] = gdf["attributes"].apply(json.dumps)

    gdf.to_postgis(
        name="spatial_layer",
        con=repository.engine,
        schema="nrf_reference",
        if_exists="append",
        index=False,
        dtype={"attributes": JSONB},
    )

    return gdf


@pytest.fixture
def sample_gcn_ponds(repository: Repository) -> gpd.GeoDataFrame:
    """Load minimal GCN ponds test data.

    Creates 3 pond points for testing GCN assessments.
    """
    from shapely.geometry import Point
    from sqlalchemy.dialects.postgresql import JSONB

    ponds = [
        # Pond 1 in Red zone
        {
            "id": uuid4(),
            "layer_type": SpatialLayerType.GCN_PONDS.name,
            "version": 1,
            "geometry": Point(450200, 100200),
            "attributes": {"pond_id": "POND_001"},
        },
        # Pond 2 in Amber zone
        {
            "id": uuid4(),
            "layer_type": SpatialLayerType.GCN_PONDS.name,
            "version": 1,
            "geometry": Point(450700, 100200),
            "attributes": {"pond_id": "POND_002"},
        },
        # Pond 3 in Green zone
        {
            "id": uuid4(),
            "layer_type": SpatialLayerType.GCN_PONDS.name,
            "version": 1,
            "geometry": Point(450500, 100700),
            "attributes": {"pond_id": "POND_003"},
        },
    ]

    gdf = gpd.GeoDataFrame(ponds, crs=CRS_BNG)

    # Convert attributes dict to JSON string for JSONB column
    import json

    gdf["attributes"] = gdf["attributes"].apply(json.dumps)

    gdf.to_postgis(
        name="spatial_layer",
        con=repository.engine,
        schema="nrf_reference",
        if_exists="append",
        index=False,
        dtype={"attributes": JSONB},
    )

    return gdf


@pytest.fixture
def sample_edp_edges(repository: Repository) -> gpd.GeoDataFrame:
    """Load minimal EDP edges test data (placeholder - not yet used)."""
    from shapely.geometry import LineString
    from sqlalchemy.dialects.postgresql import JSONB

    edges = [
        {
            "id": uuid4(),
            "layer_type": SpatialLayerType.EDP_EDGES.name,
            "version": 1,
            "geometry": LineString([(450000, 100500), (451000, 100500)]),
            "attributes": {},
        }
    ]

    gdf = gpd.GeoDataFrame(edges, crs=CRS_BNG)

    # Convert attributes dict to JSON string for JSONB column
    import json

    gdf["attributes"] = gdf["attributes"].apply(json.dumps)

    gdf.to_postgis(
        name="spatial_layer",
        con=repository.engine,
        schema="nrf_reference",
        if_exists="append",
        index=False,
        dtype={"attributes": JSONB},
    )

    return gdf


@pytest.fixture
def test_data_dir() -> Path:
    """Path to test data directory.

    Returns:
        Path to tests/data directory
    """
    return Path(__file__).parent.parent / "data"
