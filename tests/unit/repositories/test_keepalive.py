"""Unit tests for the background DB pool keepalive."""

import asyncio
import time

import pytest

from app.repositories import keepalive as keepalive_module


@pytest.mark.asyncio
async def test_loop_refreshes_capped_slots_once_per_interval(mocker):
    refresh = mocker.patch.object(keepalive_module, "refresh_shared_engine_pool")
    mocker.patch.object(
        keepalive_module, "_wait_for_stop", side_effect=[False, False, True]
    )

    await keepalive_module.keepalive_loop(240, 3, asyncio.Event())

    assert refresh.call_count == 2
    assert refresh.call_args.args == (3,)


@pytest.mark.asyncio
async def test_loop_survives_a_failing_tick(mocker):
    """A DB blip must not kill the keepalive."""
    refresh = mocker.patch.object(
        keepalive_module,
        "refresh_shared_engine_pool",
        side_effect=[RuntimeError("db unreachable"), None],
    )
    mocker.patch.object(
        keepalive_module, "_wait_for_stop", side_effect=[False, False, True]
    )

    await keepalive_module.keepalive_loop(240, 3, asyncio.Event())

    assert refresh.call_count == 2


@pytest.mark.asyncio
async def test_loop_waits_for_a_jittered_interval(mocker):
    """Replicas booted together must not phase-align their reconnect bursts."""
    mocker.patch.object(keepalive_module, "refresh_shared_engine_pool")
    mocker.patch.object(keepalive_module, "_next_delay", return_value=211.0)
    wait = mocker.patch.object(
        keepalive_module, "_wait_for_stop", side_effect=[False, True]
    )

    await keepalive_module.keepalive_loop(240, 3, asyncio.Event())

    assert [call.args[1] for call in wait.call_args_list] == [211.0, 211.0]


def test_next_delay_stays_within_ten_percent_of_the_interval():
    delays = [keepalive_module._next_delay(240) for _ in range(200)]

    assert all(216 <= d <= 264 for d in delays)
    assert len(set(delays)) > 1  # jittered, not a fixed offset


@pytest.mark.asyncio
async def test_wait_for_stop_returns_false_when_the_interval_elapses():
    assert await keepalive_module._wait_for_stop(asyncio.Event(), 0.01) is False


@pytest.mark.asyncio
async def test_wait_for_stop_returns_true_as_soon_as_stop_is_signalled():
    stop_signal = asyncio.Event()
    stop_signal.set()

    started = time.monotonic()
    assert await keepalive_module._wait_for_stop(stop_signal, 30) is True
    assert time.monotonic() - started < 1  # did not sit out the interval


@pytest.mark.asyncio
async def test_start_returns_nothing_when_interval_disabled(mocker):
    refresh = mocker.patch.object(keepalive_module, "refresh_shared_engine_pool")

    assert keepalive_module.start_keepalive(0, warm_slots=3) is None

    await asyncio.sleep(0.05)
    refresh.assert_not_called()


@pytest.mark.asyncio
async def test_stop_waits_for_an_in_flight_refresh_to_finish(mocker):
    """to_thread survives task cancellation, so shutdown must not race it."""
    finished = []

    def slow_refresh(_warm_slots):
        time.sleep(0.3)
        finished.append("refresh")

    mocker.patch.object(
        keepalive_module, "refresh_shared_engine_pool", side_effect=slow_refresh
    )
    mocker.patch.object(keepalive_module, "_next_delay", return_value=0.0)

    keepalive = keepalive_module.start_keepalive(240, warm_slots=3)
    await asyncio.sleep(0.05)  # let the refresh reach the worker thread
    assert await keepalive_module.stop_keepalive(keepalive) is True

    assert finished == ["refresh"]
    assert keepalive.task.done()
    assert not keepalive.task.cancelled()


@pytest.mark.asyncio
async def test_stop_abandons_a_refresh_that_overruns_the_timeout(mocker):
    """Cancelling cannot stop the worker thread, so the caller must be told."""
    finished = []

    def slow_refresh(_warm_slots):
        time.sleep(0.3)
        finished.append("refresh")

    mocker.patch.object(keepalive_module, "_STOP_TIMEOUT_SECONDS", 0.05)
    mocker.patch.object(
        keepalive_module, "refresh_shared_engine_pool", side_effect=slow_refresh
    )
    mocker.patch.object(keepalive_module, "_next_delay", return_value=0.0)

    keepalive = keepalive_module.start_keepalive(240, warm_slots=3)
    await asyncio.sleep(0.05)
    assert await keepalive_module.stop_keepalive(keepalive) is False

    assert keepalive.task.cancelled()
    assert finished == []  # the thread is still in the refresh


@pytest.mark.asyncio
async def test_stop_tolerates_a_disabled_keepalive():
    assert await keepalive_module.stop_keepalive(None) is True
