import pytest

from app.aws.resilience import Backoff, ReadinessGate


def test_backoff_full_jitter_within_bounds():
    # rng fixed at 1.0 => delay == raw exponential value
    b = Backoff(base=1.0, factor=2.0, cap=60.0, rng=lambda: 1.0)
    assert b.next_delay() == pytest.approx(1.0)  # 1 * 2**0
    assert b.next_delay() == pytest.approx(2.0)  # 1 * 2**1
    assert b.next_delay() == pytest.approx(4.0)  # 1 * 2**2


def test_backoff_caps():
    b = Backoff(base=1.0, factor=2.0, cap=5.0, rng=lambda: 1.0)
    for _ in range(10):
        last = b.next_delay()
    assert last == pytest.approx(5.0)


def test_backoff_jitter_scales_raw():
    b = Backoff(base=10.0, factor=2.0, cap=100.0, rng=lambda: 0.5)
    assert b.next_delay() == pytest.approx(5.0)  # 0.5 * 10


def test_backoff_reset():
    b = Backoff(base=1.0, factor=2.0, cap=60.0, rng=lambda: 1.0)
    b.next_delay()
    b.next_delay()
    b.reset()
    assert b.next_delay() == pytest.approx(1.0)


def test_gate_open_when_all_probes_true():
    gate = ReadinessGate(checks=[lambda: True, lambda: True])
    assert gate.ok() is True


def test_gate_closed_when_any_probe_false():
    gate = ReadinessGate(checks=[lambda: True, lambda: False])
    assert gate.ok() is False


def test_gate_closed_when_probe_raises():
    def boom():
        msg = "db down"
        raise RuntimeError(msg)

    gate = ReadinessGate(checks=[boom])
    assert gate.ok() is False


def test_gate_caches_within_ttl():
    calls = {"n": 0}
    now = {"t": 0.0}

    def probe():
        calls["n"] += 1
        return True

    gate = ReadinessGate(checks=[probe], ttl_seconds=10.0, clock=lambda: now["t"])
    assert gate.ok() is True
    assert gate.ok() is True  # within TTL, cached
    assert calls["n"] == 1
    now["t"] = 11.0  # TTL elapsed
    assert gate.ok() is True
    assert calls["n"] == 2
