"""Integration test fixtures for PostGIS repository tests."""

import subprocess
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import geopandas as gpd
import pytest
from shapely.geometry import MultiPolygon, Polygon
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from app.repositories.repository import Repository

CRS_BNG = "EPSG:27700"
TEST_DATABASE = "test_nrf_impact"

# Every reference table an integration test may leave committed rows in.
# Truncated before each test that declares `clean_reference_tables`.
REFERENCE_TABLES = (
    "coefficient_layer",
    "nn_catchments",
    "wwtw_catchments",
    "lpa_boundaries",
    "subcatchments",
    "gcn_risk_zones",
    "gcn_ponds",
    "edp_edges",
    "edp_boundary_layer",
    "edp_excluded_areas",
    "data_active_version",
    "lookup_table",
    "levy_charges",
    "audit_levy_calculations",
    "levy_inflation_index",
)


@pytest.fixture(scope="session")
def test_engine() -> Iterator[Engine]:
    """Create the test_nrf_impact database, migrate it to head with Alembic (the
    same migration logic as production) and drop it after all tests complete.
    """
    with scratch_database(TEST_DATABASE) as engine:
        run_alembic(TEST_DATABASE, "upgrade", "head")
        yield engine


@pytest.fixture
def clean_reference_tables(test_engine: Engine) -> None:
    """Truncate every reference table before the test runs.

    Cleaning at setup rather than teardown makes isolation independent of what
    the previous test did — a test that fails part-way through, or one that
    simply forgets to tidy up, cannot leak rows into the next one.

    Tests that only read `pg_temp` staging tables still need this: the QC gate's
    row_count rule compares the staged count against `public.<table>`, so more
    than `qc._RATIO_MIN_PREV_COUNT` leftover live rows makes a one-row staging
    fixture fail the percentage floor.
    """
    with test_engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        for table in REFERENCE_TABLES:
            conn.execute(text(f"TRUNCATE public.{table} CASCADE"))  # noqa: S608


@pytest.fixture
def repository(test_engine: Engine, clean_reference_tables: None) -> Repository:
    """Create Repository instance with clean database for each test."""
    return Repository(test_engine)


@pytest.fixture
def sample_coefficient_data(repository: Repository) -> gpd.GeoDataFrame:
    """Load minimal coefficient layer test data.

    Creates 3 sample coefficient polygons with distinct coefficients for
    testing spatial queries.
    """
    # Create 3 sample coefficient polygons
    polygons = [
        # Polygon 1
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
            "lu_curr_n_coeff": 15.5,
            "lu_curr_p_coeff": 1.2,
            "n_resi_coeff": 5.0,
            "p_resi_coeff": 0.5,
        },
        # Polygon 2: overlaps polygon 1
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
            "lu_curr_n_coeff": 8.0,
            "lu_curr_p_coeff": 0.8,
            "n_resi_coeff": 5.0,
            "p_resi_coeff": 0.5,
        },
        # Polygon 3: disjoint from 1 and 2
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
        schema="public",
        if_exists="append",
        index=False,
    )

    return gdf


@pytest.fixture
def sample_spatial_data(repository: Repository) -> gpd.GeoDataFrame:
    """Load minimal NN catchment test data into the dedicated nn_catchments table."""
    features = [
        {
            "id": uuid4(),
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
        {
            "id": uuid4(),
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

    gdf.to_postgis(
        name="nn_catchments",
        con=repository.engine,
        schema="public",
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
        {
            "id": uuid4(),
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
        {
            "id": uuid4(),
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
        {
            "id": uuid4(),
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

    import json

    gdf["attributes"] = gdf["attributes"].apply(json.dumps)

    gdf.to_postgis(
        name="gcn_risk_zones",
        con=repository.engine,
        schema="public",
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
        {
            "id": uuid4(),
            "version": 1,
            "geometry": Point(450200, 100200),
            "attributes": {"pond_id": "POND_001"},
        },
        {
            "id": uuid4(),
            "version": 1,
            "geometry": Point(450700, 100200),
            "attributes": {"pond_id": "POND_002"},
        },
        {
            "id": uuid4(),
            "version": 1,
            "geometry": Point(450500, 100700),
            "attributes": {"pond_id": "POND_003"},
        },
    ]

    gdf = gpd.GeoDataFrame(ponds, crs=CRS_BNG)

    import json

    gdf["attributes"] = gdf["attributes"].apply(json.dumps)

    gdf.to_postgis(
        name="gcn_ponds",
        con=repository.engine,
        schema="public",
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
            "version": 1,
            "geometry": LineString([(450000, 100500), (451000, 100500)]),
            "attributes": {},
        }
    ]

    gdf = gpd.GeoDataFrame(edges, crs=CRS_BNG)

    import json

    gdf["attributes"] = gdf["attributes"].apply(json.dumps)

    gdf.to_postgis(
        name="edp_edges",
        con=repository.engine,
        schema="public",
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


def set_active_version(repository: Repository, table: str, version: int) -> None:
    """Point `table` at `version` in data_active_version.

    Shared because the spatial-intersection tests each seed their own rows at a
    chosen version and then have to make that version the active one; the query
    under test filters on it.
    """
    with repository.session() as session:
        session.execute(
            text(
                "INSERT INTO public.data_active_version "
                "(table_name, active_version, updated_at) "
                "VALUES (:t, :v, now()) "
                "ON CONFLICT (table_name) DO UPDATE SET active_version = :v"
            ),
            {"t": table, "v": version},
        )
        session.commit()


def insert_levy_charge(
    repository: Repository,
    edp_id: int = 1,
    valid_from: date = date(2026, 1, 1),
    valid_to: date = date(2026, 12, 31),
    price: str = "2675",
) -> UUID:
    """Insert a committed levy_charges row and return its id.

    Shared because the levy endpoint, lookup and constraint tests all seed
    their own charge rows.
    """
    with repository.session() as session:
        charge_id = session.execute(
            text(
                "INSERT INTO public.levy_charges "
                "(id, edp_id, edp_name, edp_start_date, charge_valid_from, "
                "charge_valid_to, base_charge_per_unit) VALUES "
                "(gen_random_uuid(), :id, 'Test EDP', :f, :f, :t, :price) "
                "RETURNING id"
            ),
            {
                "id": edp_id,
                "f": valid_from,
                "t": valid_to,
                "price": Decimal(price),
            },
        ).scalar_one()
        session.commit()
    return charge_id


def _database_url(database: str) -> str:
    from app.config import DatabaseSettings

    db = DatabaseSettings()
    return f"postgresql+psycopg2://{db.user}@{db.host}:{db.port}/{database}"  # NOSONAR


def run_alembic(database: str, *args: str) -> None:
    """Run an Alembic command (e.g. "upgrade", "head") against `database`.

    Raises RuntimeError with Alembic's output when the command fails.
    """
    from app.config import DatabaseSettings

    db = DatabaseSettings()
    alembic_ini = Path(__file__).parent.parent.parent / "alembic.ini"
    env = {
        "DB_HOST": db.host,
        "DB_PORT": str(db.port),
        "DB_DATABASE": database,
        "DB_IAM_AUTHENTICATION": "false",
        "DB_LOCAL_PASSWORD": "",
    }
    cmd = ["alembic", "-c", str(alembic_ini), *args]  # noqa: S607
    result = subprocess.run(  # noqa: S603
        cmd,
        env={**subprocess.os.environ, **env},
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        msg = f"alembic {' '.join(args)} failed:\n{result.stdout}\n{result.stderr}"
        raise RuntimeError(msg)


def _drop_database(admin_engine: Engine, database: str) -> None:
    with admin_engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(
            text(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = :db AND pid <> pg_backend_pid()"
            ),
            {"db": database},
        )
        conn.execute(text(f"DROP DATABASE IF EXISTS {database}"))


@contextmanager
def scratch_database(database: str) -> Iterator[Engine]:
    """Create an empty PostGIS database, and drop it afterwards.

    Used for test_nrf_impact and for migration tests that need a database they
    can migrate to any revision.
    """
    admin_engine = create_engine(_database_url("postgres"))
    _drop_database(admin_engine, database)
    with admin_engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(text(f"CREATE DATABASE {database}"))

    engine = create_engine(_database_url(database))
    with engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))

    try:
        yield engine
    finally:
        engine.dispose()
        _drop_database(admin_engine, database)
        admin_engine.dispose()
