"""A database stamped onto the squashed baseline without ever running it is
missing whatever the squashed revisions created — in practice
`public.edp_boundary_layer`, added by the squashed `1bf027d04bb3`. Upgrading to
head must repair that, not leave the table missing forever.
"""

import subprocess
from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

pytestmark = pytest.mark.integration

REPAIR_DB = "test_nrf_impact_repair"
# The head that shipped before the repair migration existed: a database at this
# revision with the table missing is exactly the drift seen in the wild.
PRE_REPAIR_REVISION = "c3d7e1f2a4b6"


def _alembic(target: str, database: str) -> None:
    alembic_ini = Path(__file__).parent.parent.parent / "alembic.ini"
    env = {
        "DB_HOST": "localhost",
        "DB_PORT": "5432",
        "DB_DATABASE": database,
        "DB_IAM_AUTHENTICATION": "false",
        "DB_LOCAL_PASSWORD": "",
    }
    cmd = ["alembic", "-c", str(alembic_ini), "upgrade", target]  # noqa: S607
    result = subprocess.run(  # noqa: S603
        cmd,
        env={**subprocess.os.environ, **env},
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        msg = f"alembic upgrade {target} failed:\n{result.stdout}\n{result.stderr}"
        raise RuntimeError(msg)


@pytest.fixture
def stamped_but_incomplete_engine() -> Engine:
    """A database at the pre-repair head whose edp_boundary_layer never got
    created — the state left behind by stamping onto the squashed baseline.
    """
    admin_url = "postgresql://postgres@localhost:5432/postgres"  # NOSONAR
    admin_engine = create_engine(admin_url)
    with admin_engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(
            text(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = :db AND pid <> pg_backend_pid()"
            ),
            {"db": REPAIR_DB},
        )
        conn.execute(text(f"DROP DATABASE IF EXISTS {REPAIR_DB}"))
        conn.execute(text(f"CREATE DATABASE {REPAIR_DB}"))

    engine = create_engine(
        f"postgresql://postgres@localhost:5432/{REPAIR_DB}"
    )  # NOSONAR
    with engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))

    _alembic(PRE_REPAIR_REVISION, REPAIR_DB)

    # Simulate the drift: the squashed EDP boundary revision never ran here.
    with engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(text("DROP TABLE IF EXISTS public.edp_boundary_layer CASCADE"))

    yield engine

    engine.dispose()
    with admin_engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(
            text(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = :db AND pid <> pg_backend_pid()"
            ),
            {"db": REPAIR_DB},
        )
        conn.execute(text(f"DROP DATABASE IF EXISTS {REPAIR_DB}"))
    admin_engine.dispose()


def test_upgrade_recreates_edp_boundary_layer(stamped_but_incomplete_engine: Engine):
    _alembic("head", REPAIR_DB)

    inspector = inspect(stamped_but_incomplete_engine)
    assert "edp_boundary_layer" in inspector.get_table_names(schema="public")
    columns = {
        c["name"] for c in inspector.get_columns("edp_boundary_layer", schema="public")
    }
    assert {"id", "version", "geometry", "name", "attributes", "created_at"} <= columns


def test_upgrade_recreates_the_geometry_index(stamped_but_incomplete_engine: Engine):
    _alembic("head", REPAIR_DB)

    with stamped_but_incomplete_engine.connect() as conn:
        indexes = set(
            conn.scalars(
                text(
                    "SELECT indexname FROM pg_indexes "
                    "WHERE schemaname = 'public' AND tablename = 'edp_boundary_layer'"
                )
            )
        )
    assert "ix_public_edp_boundary_layer_geometry" in indexes


def test_upgrade_drops_the_legacy_spatial_layer_table(
    stamped_but_incomplete_engine: Engine,
):
    """Pre-squash databases still carry `public.spatial_layer`, which nothing
    reads or writes any more; upgrading to head must clear it away.
    """
    with stamped_but_incomplete_engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(
            text(
                "CREATE TABLE public.spatial_layer "
                "(id uuid PRIMARY KEY, version integer NOT NULL)"
            )
        )

    _alembic("head", REPAIR_DB)

    inspector = inspect(stamped_but_incomplete_engine)
    assert "spatial_layer" not in inspector.get_table_names(schema="public")


def test_upgrade_succeeds_where_the_legacy_table_was_never_there(
    stamped_but_incomplete_engine: Engine,
):
    """Databases migrated after the squash never had spatial_layer at all, so
    the drop must not fail on them.
    """
    _alembic("head", REPAIR_DB)

    inspector = inspect(stamped_but_incomplete_engine)
    assert "spatial_layer" not in inspector.get_table_names(schema="public")
    assert "edp_boundary_layer" in inspector.get_table_names(schema="public")


def test_upgrade_is_a_noop_when_the_table_is_already_present(
    stamped_but_incomplete_engine: Engine,
):
    """The repair must not disturb correctly-migrated databases: running it
    where the table exists (and holds rows) leaves the data alone.
    """
    _alembic("head", REPAIR_DB)
    with stamped_but_incomplete_engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(
            text(
                "INSERT INTO public.edp_boundary_layer (id, version, geometry, name) "
                "VALUES (gen_random_uuid(), 1, "
                "ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))', 27700), 'keep me')"
            )
        )

    _alembic("head", REPAIR_DB)

    with stamped_but_incomplete_engine.connect() as conn:
        names = list(conn.scalars(text("SELECT name FROM public.edp_boundary_layer")))
    assert names == ["keep me"]
