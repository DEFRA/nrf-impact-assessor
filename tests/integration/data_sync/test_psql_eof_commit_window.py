"""What `psql --single-transaction` does when its stdin ends early.

`restore_all_atomic` streams every table of a batch into one psql process and
relies on that single transaction for all-or-nothing promotion. But the parent
does not send an explicit COMMIT — it closes stdin (restore.py:397) and lets
psql commit at end of input.

That makes the *manner* of a mid-run death matter. Killing the container takes
psql down too, so the transaction aborts and rolls back. But if only the Python
parent dies (unhandled crash, OOM killer picking that PID), the pipe's write end
closes and psql sees a normal EOF — indistinguishable from an orderly finish.

These tests pin down what psql actually does at that EOF, and whether the PROMOTE
loop has a window where it commits a partial batch. Closing stdin early
reproduces the parent's death exactly: it is the same event psql observes.
"""

import os
import subprocess
import sys
import time
from uuid import uuid4

import pytest
from sqlalchemy import text

from app.config import AWSConfig, DatabaseSettings
from app.data_sync.restore import build_psql_env, post_sql, pre_sql, staging_name

pytestmark = pytest.mark.integration

_TABLES = ("nn_catchments", "lpa_boundaries")


@pytest.fixture(autouse=True)
def _env(monkeypatch, test_engine):
    monkeypatch.setenv("DB_IAM_AUTHENTICATION", "false")
    monkeypatch.setenv("DB_DATABASE", "test_nrf_impact")
    yield
    with test_engine.begin() as conn:
        conn.execute(text("DELETE FROM public.data_active_version"))
        conn.execute(text("DELETE FROM public.data_load_history"))
        conn.execute(text("DELETE FROM public.data_sync_run"))
        for table in _TABLES:
            conn.execute(text(f"TRUNCATE public.{table}"))


def _psql():
    """A psql process configured exactly as restore_all_atomic configures it."""
    return subprocess.Popen(  # noqa: S603
        ["psql", "-v", "ON_ERROR_STOP=1", "--single-transaction", "--quiet"],  # noqa: S607
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        env=build_psql_env(DatabaseSettings(), AWSConfig().region),
    )


def _seed_live_row(test_engine, table: str) -> None:
    """One row at version 1, so the table has a prior version to promote over."""
    with test_engine.begin() as conn:
        conn.execute(
            text(
                f"INSERT INTO public.{table} "  # noqa: S608
                "(id, version, geometry, name, attributes) VALUES "
                "(gen_random_uuid(), 1, "
                "ST_GeomFromText('POLYGON((0 0,0 1,1 1,1 0,0 0))', 27700), "
                "'seed', '{}')"
            )
        )


def _active_versions(test_engine) -> dict[str, int]:
    with test_engine.connect() as conn:
        rows = conn.execute(
            text("SELECT table_name, active_version FROM public.data_active_version")
        ).all()
    return {r[0]: r[1] for r in rows}


def test_psql_commits_work_on_stdin_eof(test_engine):
    """EOF on stdin commits — it does not abort.

    This is the premise the partial-promotion window rests on. If psql rolled
    back at EOF instead, a parent-only death would be harmless.
    """
    proc = _psql()
    proc.stdin.write(
        b"INSERT INTO public.data_sync_run (id, status) "
        b"VALUES (gen_random_uuid(), 'eof-probe');\n"
    )
    proc.stdin.close()  # the parent dying, from psql's point of view
    _, stderr = proc.communicate()

    assert proc.returncode == 0, stderr.decode(errors="replace")
    with test_engine.connect() as conn:
        count = conn.scalar(
            text("SELECT count(*) FROM public.data_sync_run WHERE status = 'eof-probe'")
        )
    assert count == 1, "psql discarded the work at EOF rather than committing it"


def test_eof_between_promotes_commits_a_partial_batch(test_engine):
    """The window: stdin ends after table 1's PROMOTE but before table 2's.

    Mirrors restore_all_atomic's structure — both tables staged first, then
    post_sql written per table in a loop (restore.py:395-396) — and cuts the
    stream between the two promotes.
    """
    run_id = uuid4()
    with test_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO public.data_sync_run (id, status) VALUES (:id, 'running')"
            ),
            {"id": str(run_id)},
        )
    for table in _TABLES:
        _seed_live_row(test_engine, table)

    proc = _psql()
    # STAGE both tables, as the real loop does before any promotion. Staging is
    # fed from the live row rather than a gzipped dump: the dump only determines
    # what lands in staging, and the EOF window is entirely about PROMOTE.
    for table in _TABLES:
        proc.stdin.write(pre_sql(table).encode())
        proc.stdin.write(
            f"INSERT INTO pg_temp.{staging_name(table)} "  # noqa: S608
            f"SELECT * FROM public.{table} LIMIT 1;\n".encode()
        )
    # PROMOTE table 1 only, then the parent dies.
    proc.stdin.write(post_sql(_TABLES[0], str(run_id), "k", "e", "v2").encode())
    proc.stdin.close()
    _, stderr = proc.communicate()

    assert proc.returncode == 0, stderr.decode(errors="replace")

    active = _active_versions(test_engine)
    assert active == {_TABLES[0]: 2}, (
        "expected exactly one table promoted, leaving the batch half-applied; "
        f"got {active}"
    )


# A parent that has written every PROMOTE statement before dying. Writes are
# small and go into the kernel pipe buffer, so the question is whether they
# survive the writer being killed — that is what bounds how wide the window
# above really is. SIGKILLs itself without closing stdin or waiting for psql.
_SUICIDAL_PARENT = """
import os, signal, subprocess, sys
sys.path.insert(0, {repo!r})
from app.config import AWSConfig, DatabaseSettings
from app.data_sync.restore import build_psql_env, post_sql, pre_sql, staging_name

tables = {tables!r}
run_id = {run_id!r}
proc = subprocess.Popen(
    ["psql", "-v", "ON_ERROR_STOP=1", "--single-transaction", "--quiet"],
    stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    env=build_psql_env(DatabaseSettings(), AWSConfig().region),
)
for t in tables:
    proc.stdin.write(pre_sql(t).encode())
    proc.stdin.write(
        ("INSERT INTO pg_temp." + staging_name(t)
         + " SELECT * FROM public." + t + " LIMIT 1;\\n").encode()
    )
for t in tables:
    proc.stdin.write(post_sql(t, run_id, "k", "e", "v2").encode())
proc.stdin.flush()
os.kill(os.getpid(), signal.SIGKILL)
"""


def test_buffered_promotes_survive_a_real_sigkill(test_engine, tmp_path):
    """A SIGKILLed parent still gets a *complete* batch committed.

    The parent is killed outright — no clean shutdown, no stdin close, psql
    orphaned and reparented. Because the PROMOTE statements were already in the
    kernel pipe buffer, psql drains them, sees EOF and commits everything. So the
    window is not "any death during PROMOTE"; it is only a death in the gap
    between two consecutive small writes.
    """
    run_id = uuid4()
    with test_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO public.data_sync_run (id, status) VALUES (:id, 'running')"
            ),
            {"id": str(run_id)},
        )
    for table in _TABLES:
        _seed_live_row(test_engine, table)

    script = tmp_path / "parent.py"
    script.write_text(
        _SUICIDAL_PARENT.format(
            repo=os.getcwd(), tables=list(_TABLES), run_id=str(run_id)
        )
    )
    parent = subprocess.run(  # noqa: S603
        [sys.executable, str(script)], capture_output=True, check=False
    )
    assert parent.returncode == -9, (
        f"parent did not die by SIGKILL: rc={parent.returncode} "
        f"{parent.stderr.decode(errors='replace')}"
    )

    # psql is orphaned, so wait for it to drain the buffer and commit.
    deadline = time.time() + 30
    while time.time() < deadline:
        if len(_active_versions(test_engine)) == len(_TABLES):
            break
        time.sleep(0.2)

    assert _active_versions(test_engine) == dict.fromkeys(_TABLES, 2), (
        "buffered PROMOTE statements did not survive the parent's death"
    )
