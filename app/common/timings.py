"""Per-run phase timing collection.

Phases are timed with :func:`phase` and accumulated on a collector held in a
context variable, so nested code (the repository, for example) can contribute
timings without threading an object through every call. The top-level caller
opens :func:`collect`, then emits a single summary line via
:meth:`TimingCollector.summary` instead of one log record per phase.

Outside a ``collect()`` block ``phase()`` and ``note()`` are no-ops, so the
helpers stay usable from tests and standalone calls.
"""

import time
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any

__all__ = ["TimingCollector", "collect", "phase", "record", "note"]


@dataclass
class _Node:
    name: str
    seconds: float = 0.0
    children: list[_Node] = field(default_factory=list)
    notes: dict[str, Any] = field(default_factory=dict)


class TimingCollector:
    """Accumulates a tree of phase timings for one run."""

    def __init__(self) -> None:
        self._root = _Node("total")
        self._stack = [self._root]

    @contextmanager
    def phase(self, name: str) -> Iterator[_Node]:
        node = _Node(name)
        self._stack[-1].children.append(node)
        self._stack.append(node)
        t0 = time.perf_counter()
        try:
            yield node
        finally:
            node.seconds = time.perf_counter() - t0
            self._stack.pop()

    def record(self, name: str, seconds: float) -> None:
        """Record an already-measured sub-step of the current phase."""
        self._stack[-1].children.append(_Node(name, seconds))

    def note(self, key: str, value: Any) -> None:
        """Attach a detail (row counts, feature counts) to the current phase."""
        self._stack[-1].notes[key] = value

    @property
    def total_seconds(self) -> float:
        return self._root.seconds

    def summary(self) -> str:
        """One-line phase breakdown, e.g. ``spatial=0.129s[setup=0.076s] ...``.

        The overall total is not included; read it from :attr:`total_seconds`.
        """
        return " ".join(_render_inner(self._root))


def _render(node: _Node) -> str:
    inner = _render_inner(node)
    rendered = f"{node.name}={node.seconds:.3f}s"
    return f"{rendered}[{' '.join(inner)}]" if inner else rendered


def _render_inner(node: _Node) -> list[str]:
    return [f"{k}={v}" for k, v in node.notes.items()] + [
        _render(child) for child in node.children
    ]


_current: ContextVar[TimingCollector | None] = ContextVar("phase_timings", default=None)


@contextmanager
def collect() -> Iterator[TimingCollector]:
    """Collect timings for the enclosed block.

    The collector's total is only final once the block exits, so log the
    summary after the ``with`` statement.
    """
    collector = TimingCollector()
    token = _current.set(collector)
    t0 = time.perf_counter()
    try:
        yield collector
    finally:
        collector._root.seconds = time.perf_counter() - t0
        _current.reset(token)


@contextmanager
def phase(name: str) -> Iterator[None]:
    """Time the enclosed block as a phase of the active collector, if any."""
    collector = _current.get()
    if collector is None:
        yield
        return
    with collector.phase(name):
        yield


def record(name: str, seconds: float) -> None:
    """Record a measured sub-step on the active phase, if a collector is active."""
    collector = _current.get()
    if collector is not None:
        collector.record(name, seconds)


def note(key: str, value: Any) -> None:
    """Attach a detail to the active phase, if a collector is active."""
    collector = _current.get()
    if collector is not None:
        collector.note(key, value)
