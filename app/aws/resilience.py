"""Pure, unit-testable resilience helpers for the SQS consumer loop."""

import logging
import random
import time
from collections.abc import Callable

logger = logging.getLogger(__name__)


class Backoff:
    """Capped exponential backoff with full jitter.

    `next_delay()` returns a delay in [0, raw) where raw grows exponentially per
    attempt up to `cap`; the caller passes it to `time.sleep`. `reset()` returns
    to the base delay (call after a successful poll).
    """

    def __init__(
        self,
        base: float = 1.0,
        factor: float = 2.0,
        cap: float = 60.0,
        rng: Callable[[], float] = random.random,
    ):
        self._base = base
        self._factor = factor
        self._cap = cap
        self._rng = rng
        self._attempt = 0

    def next_delay(self) -> float:
        raw = min(self._cap, self._base * (self._factor**self._attempt))
        self._attempt += 1
        return self._rng() * raw

    def reset(self) -> None:
        self._attempt = 0


class ReadinessGate:
    """AND of cached readiness probes: 'can any job possibly succeed now?'

    Each probe is a zero-arg callable returning bool. A probe returning False or
    raising closes the gate. Results are cached for `ttl_seconds` so the probes
    (DB / reference-data checks) do not run on every consumer loop iteration.
    """

    def __init__(
        self,
        checks: list[Callable[[], bool]],
        ttl_seconds: float = 15.0,
        clock: Callable[[], float] = time.monotonic,
    ):
        self._checks = list(checks)
        self._ttl = ttl_seconds
        self._clock = clock
        self._cached: bool | None = None
        self._checked_at: float | None = None

    def ok(self) -> bool:
        now = self._clock()
        # _cached and _checked_at are always assigned together, so a set
        # timestamp implies a cached result.
        if self._checked_at is not None and now - self._checked_at < self._ttl:
            return self._cached

        result = True
        for check in self._checks:
            try:
                if not check():
                    result = False
                    break
            except Exception:  # noqa: BLE001
                logger.warning(
                    "readiness probe raised; treating as not-ready", exc_info=True
                )
                result = False
                break

        self._cached = result
        self._checked_at = now
        return result
