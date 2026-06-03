from app.data_sync.restore import build_psql_env, wrap_sql


def test_wrap_sql_brackets_data_with_txn():
    pre, post = wrap_sql(
        table="nn_catchments",
        drop_index_sql=["DROP INDEX IF EXISTS public.ix_nn;"],
        create_index_sql=[
            "CREATE INDEX ix_nn ON public.nn_catchments USING GIST (geometry);"
        ],
    )
    assert pre.startswith("BEGIN;")
    assert "DROP INDEX IF EXISTS public.ix_nn;" in pre
    assert "TRUNCATE public.nn_catchments;" in pre
    assert "CREATE INDEX ix_nn" in post
    assert post.strip().endswith("COMMIT;")


def test_build_psql_env_local_password(monkeypatch):
    from app.config import DatabaseSettings

    settings = DatabaseSettings(
        host="localhost",
        port=5434,
        database="nrf_impact",
        user="postgres",
        iam_authentication=False,
        local_password="pw",  # noqa: S106
    )
    env = build_psql_env(settings, region="eu-west-2")
    assert env["PGHOST"] == "localhost"
    assert env["PGPORT"] == "5434"
    assert env["PGDATABASE"] == "nrf_impact"
    assert env["PGUSER"] == "postgres"
    assert env["PGPASSWORD"] == "pw"
