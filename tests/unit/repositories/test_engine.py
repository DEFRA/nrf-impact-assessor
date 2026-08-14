"""Unit tests for the IAM auth token cache in app.repositories.engine."""

from unittest.mock import patch

import pytest

from app.config import DatabaseSettings
from app.repositories import engine as engine_module


@pytest.fixture(autouse=True)
def clear_token_cache():
    engine_module._token_cache.clear()
    yield
    engine_module._token_cache.clear()


def _settings(**overrides) -> DatabaseSettings:
    defaults: dict = {"host": "db.example.com", "port": 5432, "user": "app_user"}
    defaults.update(overrides)
    return DatabaseSettings(**defaults)


def test_token_reused_within_ttl():
    with patch.object(
        engine_module, "_generate_iam_auth_token", side_effect=["tok1", "tok2"]
    ) as gen:
        first = engine_module._get_iam_auth_token(_settings(), "eu-west-2")
        second = engine_module._get_iam_auth_token(_settings(), "eu-west-2")
    assert first == second == "tok1"
    assert gen.call_count == 1


def test_token_regenerated_after_ttl():
    with (
        patch.object(
            engine_module, "_generate_iam_auth_token", side_effect=["tok1", "tok2"]
        ) as gen,
        patch.object(
            engine_module.time,
            "monotonic",
            side_effect=[0.0, engine_module.IAM_TOKEN_CACHE_SECONDS + 1.0],
        ),
    ):
        first = engine_module._get_iam_auth_token(_settings(), "eu-west-2")
        second = engine_module._get_iam_auth_token(_settings(), "eu-west-2")
    assert (first, second) == ("tok1", "tok2")
    assert gen.call_count == 2


def test_token_cache_keyed_per_target():
    with patch.object(
        engine_module, "_generate_iam_auth_token", side_effect=["tok1", "tok2"]
    ) as gen:
        first = engine_module._get_iam_auth_token(_settings(user="user_a"), "eu-west-2")
        second = engine_module._get_iam_auth_token(
            _settings(user="user_b"), "eu-west-2"
        )
    assert (first, second) == ("tok1", "tok2")
    assert gen.call_count == 2


# ---------------------------------------------------------------------------
# Pool warmer
# ---------------------------------------------------------------------------


class _FakeConnection:
    """Records ping/close events on a shared log."""

    def __init__(self, name: str, events: list[str], ping_error: Exception | None):
        self.name = name
        self._events = events
        self._ping_error = ping_error

    def execute(self, _statement):
        self._events.append(f"ping:{self.name}")
        if self._ping_error is not None:
            raise self._ping_error

    def close(self):
        self._events.append(f"close:{self.name}")


class _FakePool:
    def __init__(self, size: int, checkedout: int, checkedin: int = 0):
        self._size = size
        self._checkedout = checkedout
        self._checkedin = checkedin

    def size(self):
        return self._size

    def checkedout(self):
        return self._checkedout

    def checkedin(self):
        return self._checkedin


class _FakeEngine:
    def __init__(self, size: int, checkedout: int = 0, ping_errors=()):
        self.pool = _FakePool(size, checkedout)
        self.events: list[str] = []
        self._ping_errors = list(ping_errors)
        self.connections: list[_FakeConnection] = []

    def connect(self):
        name = str(len(self.connections))
        self.events.append(f"connect:{name}")
        error = self._ping_errors.pop(0) if self._ping_errors else None
        conn = _FakeConnection(name, self.events, error)
        self.connections.append(conn)
        return conn


def test_warm_pool_pings_every_idle_slot():
    fake = _FakeEngine(size=10, checkedout=3)
    with patch.object(engine_module, "get_shared_engine", return_value=fake):
        engine_module.refresh_shared_engine_pool()

    assert len(fake.connections) == 7
    assert fake.events.count("ping:0") == 1
    assert sum(e.startswith("close:") for e in fake.events) == 7


def test_warm_pool_holds_all_connections_before_closing_any():
    """Sequential checkout/return would reuse one connection."""
    fake = _FakeEngine(size=4)
    with patch.object(engine_module, "get_shared_engine", return_value=fake):
        engine_module.refresh_shared_engine_pool()

    first_close = next(i for i, e in enumerate(fake.events) if e.startswith("close:"))
    last_connect = max(i for i, e in enumerate(fake.events) if e.startswith("connect:"))
    assert last_connect < first_close


def test_warm_pool_skips_when_pool_fully_checked_out():
    fake = _FakeEngine(size=5, checkedout=5)
    with patch.object(engine_module, "get_shared_engine", return_value=fake):
        engine_module.refresh_shared_engine_pool()

    assert fake.connections == []


def test_warm_pool_closes_connections_when_a_ping_fails():
    fake = _FakeEngine(size=3, ping_errors=[None, RuntimeError("connection reset")])
    with (
        patch.object(engine_module, "get_shared_engine", return_value=fake),
        pytest.raises(RuntimeError, match="connection reset"),
    ):
        engine_module.refresh_shared_engine_pool()

    assert sum(e.startswith("close:") for e in fake.events) == 2


def test_warm_pool_caps_slots_at_max():
    """A stateless service autoscales, so the per-replica floor must be bounded."""
    fake = _FakeEngine(size=10, checkedout=0)
    with patch.object(engine_module, "get_shared_engine", return_value=fake):
        engine_module.refresh_shared_engine_pool(max_slots=3)

    assert len(fake.connections) == 3


def test_warm_pool_cap_never_exceeds_idle_slots():
    fake = _FakeEngine(size=10, checkedout=8)
    with patch.object(engine_module, "get_shared_engine", return_value=fake):
        engine_module.refresh_shared_engine_pool(max_slots=3)

    assert len(fake.connections) == 2


class _AgingConnection:
    def __init__(self, engine, created_at):
        self._engine = engine
        self.created_at = created_at
        self._invalid = False

    def execute(self, _statement):
        pass

    def invalidate(self):
        self._invalid = True

    def close(self):
        self._engine.pool.checked_out.remove(self.created_at)
        if not self._invalid:
            self._engine.pool.idle.append(self.created_at)


class _AgingPool:
    """Models QueuePool: idle connections keep their creation time and are
    discarded on checkout once older than pool_recycle."""

    def __init__(self, size, recycle):
        self._size = size
        self.recycle = recycle
        self.idle: list[float] = []
        self.checked_out: list[float] = []
        self.recycled_on_checkout = 0

    def size(self):
        return self._size

    def checkedout(self):
        return len(self.checked_out)

    def checkedin(self):
        return len(self.idle)


class _AgingEngine:
    def __init__(self, clock, size=10, recycle=600):
        self._clock = clock
        self.pool = _AgingPool(size, recycle)

    def connect(self):
        now = self._clock()
        while self.pool.idle:
            created = self.pool.idle.pop(0)
            if now - created > self.pool.recycle:
                self.pool.recycled_on_checkout += 1
                continue
            self.pool.checked_out.append(created)
            return _AgingConnection(self, created)
        self.pool.checked_out.append(now)
        return _AgingConnection(self, now)


def _run_ticks(engine, now, ticks=15):
    """Run ``ticks`` keepalive ticks, each followed by a request."""
    for tick in range(1, ticks + 1):  # 15 ticks x 240s = one hour
        now[0] = tick * 240.0
        engine_module.refresh_shared_engine_pool(max_slots=3)
        now[0] += 239.0  # a request lands just before the next tick
        conn = engine.connect()
        conn.execute("SELECT 1")
        conn.close()


def test_warmed_slots_never_age_past_pool_recycle():
    """A request must never be the one to discover an aged-out connection."""
    now = [0.0]
    engine = _AgingEngine(lambda: now[0], size=10, recycle=600)

    with patch.object(engine_module, "get_shared_engine", return_value=engine):
        _run_ticks(engine, now)

    assert engine.pool.recycled_on_checkout == 0


def test_burst_expanded_pool_never_ages_past_pool_recycle():
    """10 connections at 3 per 240s tick cycle in 800s — past the 600s recycle
    window, so a capped tick must drop the excess rather than rotate it."""
    now = [0.0]
    engine = _AgingEngine(lambda: now[0], size=10, recycle=600)

    # A burst checks out the whole pool at once, then returns it all idle.
    burst = [engine.connect() for _ in range(10)]
    for conn in burst:
        conn.close()
    assert engine.pool.checkedin() == 10

    with patch.object(engine_module, "get_shared_engine", return_value=engine):
        _run_ticks(engine, now)

    assert engine.pool.recycled_on_checkout == 0


def test_warm_pool_discards_idle_connections_beyond_the_cap():
    """The pool settles back to the warm-slot floor after a burst expands it."""
    now = [0.0]
    engine = _AgingEngine(lambda: now[0], size=10, recycle=600)
    burst = [engine.connect() for _ in range(10)]
    for conn in burst:
        conn.close()

    with patch.object(engine_module, "get_shared_engine", return_value=engine):
        now[0] = 240.0
        engine_module.refresh_shared_engine_pool(max_slots=3)

    assert engine.pool.checkedin() == 3
    assert all(created == 240.0 for created in engine.pool.idle)
