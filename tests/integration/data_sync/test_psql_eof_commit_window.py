"""The restore's commit protocol, probed at the psql level.

`restore_all_atomic` streams every table of a batch into one psql process. The
protocol is an explicit `BEGIN` (plus psql's own advisory lock) first and an
explicit `COMMIT` last — deliberately NOT `psql --single-transaction`, which
commits at end of input.

That distinction is what makes the *manner* of a mid-run death safe. Killing the
container takes psql down too, so the transaction aborts either way. But if only
the Python parent dies (unhandled crash, OOM killer picking that PID), the pipe's
write end closes and psql sees a normal EOF — indistinguishable from an orderly
finish. Under `--single-transaction` that EOF committed whatever had arrived,
which for a death partway through the PROMOTE loop meant a half-applied batch at
mixed versions. Under the explicit protocol the COMMIT never arrives, so the
server rolls the whole batch back.

These tests pin down both halves: that `--single-transaction` really did commit
at EOF (the regression these guard against), and that the protocol the code now
uses rolls back instead. Closing stdin early reproduces the parent's death
exactly: it is the same event psql observes.
"""

import os
import subprocess
import sys
import time
from uuid import uuid4

import pytest
from sqlalchemy import text

from app.config import AWSConfig, DatabaseSettings, DataSyncConfig
from app.data_sync.restore import (
    begin_sql,
    build_psql_env,
    commit_sql,
    post_sql,
    pre_sql,
    restore_lock_key,
    staging_name,
)

pytestmark = pytest.mark.integration

_TABLES = ("nn_catchments", "lpa_boundaries")

_PSQL_CMD = ["psql", "-v", "ON_ERROR_STOP=1", "--quiet"]
_LEGACY_PSQL_CMD = ["psql", "-v", "ON_ERROR_STOP=1", "--single-transaction", "--quiet"]


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


def _psql(cmd=None):
    """A psql process configured exactly as restore_all_atomic configures it."""
    return subprocess.Popen(  # noqa: S603
        cmd or _PSQL_CMD,
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        env=build_psql_env(DatabaseSettings(), AWSConfig().region),
    )


def _begin() -> bytes:
    return begin_sql(restore_lock_key(DataSyncConfig().lock_key)).encode()


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


def _start_run(test_engine):
    run_id = uuid4()
    with test_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO public.data_sync_run (id, status) VALUES (:id, 'running')"
            ),
            {"id": str(run_id)},
        )
    return run_id


def test_single_transaction_commits_work_on_stdin_eof(test_engine):
    """Why the restore does NOT use --single-transaction.

    Documents the behaviour being avoided: under that flag EOF on stdin commits
    rather than aborting. If psql rolled back at EOF instead, a parent-only death
    would have been harmless and the explicit protocol unnecessary.
    """
    proc = _psql(_LEGACY_PSQL_CMD)
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
    assert count == 1, "premise broken: --single-transaction no longer commits at EOF"


def test_explicit_protocol_discards_work_on_stdin_eof(test_engine):
    """The protocol the restore actually uses: EOF before COMMIT rolls back."""
    proc = _psql()
    proc.stdin.write(_begin())
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
    assert count == 0, "work committed despite the COMMIT never being written"


def test_eof_between_promotes_rolls_back_the_whole_batch(test_engine):
    """The former window: stdin ends after table 1's PROMOTE but before table 2's.

    Mirrors restore_all_atomic's structure — BEGIN, both tables staged, then
    post_sql written per table in a loop — and cuts the stream between the two
    promotes. Under --single-transaction this committed table 1 alone, leaving
    the batch half-applied; under the explicit protocol neither is promoted.
    """
    run_id = _start_run(test_engine)
    for table in _TABLES:
        _seed_live_row(test_engine, table)

    proc = _psql()
    proc.stdin.write(_begin())
    # STAGE both tables, as the real loop does before any promotion. Staging is
    # fed from the live row rather than a gzipped dump: the dump only determines
    # what lands in staging, and the EOF window is entirely about PROMOTE.
    for table in _TABLES:
        proc.stdin.write(pre_sql(table).encode())
        proc.stdin.write(
            f"INSERT INTO pg_temp.{staging_name(table)} "  # noqa: S608
            f"SELECT * FROM public.{table} LIMIT 1;\n".encode()
        )
    # PROMOTE table 1 only, then the parent dies before COMMIT.
    proc.stdin.write(post_sql(_TABLES[0], str(run_id), "k", "e", "v2").encode())
    proc.stdin.close()
    _, stderr = proc.communicate()

    assert proc.returncode == 0, stderr.decode(errors="replace")

    active = _active_versions(test_engine)
    assert active == {}, (
        f"expected the half-applied batch to roll back entirely; got {active}"
    )


def test_full_batch_with_commit_promotes_every_table(test_engine):
    """The happy path still commits: the rollback above is not blanket breakage."""
    run_id = _start_run(test_engine)
    for table in _TABLES:
        _seed_live_row(test_engine, table)

    proc = _psql()
    proc.stdin.write(_begin())
    for table in _TABLES:
        proc.stdin.write(pre_sql(table).encode())
        proc.stdin.write(
            f"INSERT INTO pg_temp.{staging_name(table)} "  # noqa: S608
            f"SELECT * FROM public.{table} LIMIT 1;\n".encode()
        )
    for table in _TABLES:
        proc.stdin.write(post_sql(table, str(run_id), "k", "e", "v2").encode())
    proc.stdin.write(commit_sql().encode())
    proc.stdin.close()
    _, stderr = proc.communicate()

    assert proc.returncode == 0, stderr.decode(errors="replace")
    assert _active_versions(test_engine) == dict.fromkeys(_TABLES, 2)


# A parent that has written every PROMOTE statement — but not COMMIT — before
# dying. Writes are small and go into the kernel pipe buffer, so they survive the
# writer being killed; the question is whether that is enough to commit. SIGKILLs
# itself without closing stdin or waiting for psql.
_SUICIDAL_PARENT = """
import os, signal, subprocess, sys
sys.path.insert(0, {repo!r})
from app.config import AWSConfig, DatabaseSettings, DataSyncConfig
from app.data_sync.restore import (
    begin_sql, build_psql_env, post_sql, pre_sql, restore_lock_key, staging_name
)

tables = {tables!r}
run_id = {run_id!r}
proc = subprocess.Popen(
    ["psql", "-v", "ON_ERROR_STOP=1", "--quiet"],
    stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    env=build_psql_env(DatabaseSettings(), AWSConfig().region),
)
proc.stdin.write(begin_sql(restore_lock_key(DataSyncConfig().lock_key)).encode())
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


def test_buffered_promotes_without_commit_are_discarded(test_engine, tmp_path):
    """A SIGKILLed parent commits nothing, however far it got.

    The parent is killed outright — no clean shutdown, no stdin close, psql
    orphaned and reparented. Its PROMOTE statements were already in the kernel
    pipe buffer, so psql drains and executes all of them; under
    --single-transaction it then saw EOF and committed the lot. Without a COMMIT
    in that buffer the session ends with the transaction open and the server
    rolls it back, so the buffered work never becomes visible.
    """
    run_id = _start_run(test_engine)
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

    # psql is orphaned; give it time to drain the buffer and reach EOF, so a
    # commit would have landed by now if one were coming.
    deadline = time.time() + 30
    while time.time() < deadline:
        if _active_versions(test_engine):
            break
        time.sleep(0.2)

    assert _active_versions(test_engine) == {}, (
        "buffered PROMOTE statements committed without a COMMIT"
    )
