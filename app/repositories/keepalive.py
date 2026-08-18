"""Background keepalive that reconnects aged-out pooled DB connections.

Without it, ``pool_recycle`` expiry is discovered on checkout and the reconnect
(TCP + TLS + IAM) is billed to the first request after an idle period.
"""

import asyncio
import contextlib
import random
from dataclasses import dataclass
from logging import getLogger

from app.repositories.engine import refresh_shared_engine_pool

logger = getLogger(__name__)

_TASK_NAME = "db-pool-keepalive"

# Spread reconnect bursts across replicas that booted together.
_JITTER_FRACTION = 0.1

# How long shutdown waits for an in-flight refresh before giving up on it.
_STOP_TIMEOUT_SECONDS = 15.0


@dataclass(frozen=True)
class Keepalive:
    """A running keepalive loop and the signal that stops it."""

    task: asyncio.Task
    stop_signal: asyncio.Event


def _next_delay(interval_seconds: int) -> float:
    """Return the interval jittered by +/-10%."""
    return interval_seconds * random.uniform(  # noqa: S311 - not cryptographic
        1 - _JITTER_FRACTION, 1 + _JITTER_FRACTION
    )


async def _wait_for_stop(stop_signal: asyncio.Event, delay: float) -> bool:
    """Wait up to ``delay`` seconds; return True if stop was signalled."""
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop_signal.wait(), delay)
    return stop_signal.is_set()


async def keepalive_loop(
    interval_seconds: int, warm_slots: int, stop_signal: asyncio.Event
) -> None:
    """Refresh up to ``warm_slots`` connections until stop is signalled.

    Stopping is a signal rather than cancellation: a refresh runs in a worker
    thread via ``to_thread``, and cancelling the awaiting task would leave that
    thread opening connections while shutdown deletes the TLS cert files it
    needs. Checking between ticks lets an in-flight refresh finish first;
    ``stop_keepalive`` only cancels as a last resort, and reports when it does.
    """
    while not await _wait_for_stop(stop_signal, _next_delay(interval_seconds)):
        try:
            # Sync SQLAlchemy calls — keep them off the event loop.
            await asyncio.to_thread(refresh_shared_engine_pool, warm_slots)
        except Exception:
            # A DB blip must not kill the keepalive for the process lifetime.
            logger.exception("DB pool keepalive tick failed; continuing")


def start_keepalive(interval_seconds: int, warm_slots: int) -> Keepalive | None:
    """Start the keepalive loop, or return None when it is disabled."""
    if interval_seconds <= 0:
        logger.info("DB pool keepalive disabled (interval=%ds)", interval_seconds)
        return None
    logger.info(
        "DB pool keepalive started (interval=%ds, warm_slots=%d)",
        interval_seconds,
        warm_slots,
    )
    stop_signal = asyncio.Event()
    task = asyncio.create_task(
        keepalive_loop(interval_seconds, warm_slots, stop_signal), name=_TASK_NAME
    )
    return Keepalive(task=task, stop_signal=stop_signal)


async def stop_keepalive(keepalive: Keepalive | None) -> bool:
    """Signal the loop to stop; return True once no refresh is still running.

    False means the timeout expired and the task was cancelled, which does not
    stop the ``to_thread`` worker — the caller must treat the TLS cert files it
    needs as still in use.
    """
    if keepalive is None:
        return True
    keepalive.stop_signal.set()
    try:
        await asyncio.wait_for(asyncio.shield(keepalive.task), _STOP_TIMEOUT_SECONDS)
    except TimeoutError:
        logger.warning(
            "DB pool keepalive did not stop within %.0fs; abandoning it",
            _STOP_TIMEOUT_SECONDS,
        )
        keepalive.task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await keepalive.task
        return False
    logger.info("DB pool keepalive stopped")
    return True
