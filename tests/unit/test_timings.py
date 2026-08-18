"""Tests for the per-run phase timing collector."""

from app.common import timings


def test_summary_renders_nested_phases_and_notes():
    with timings.collect() as collected:
        with timings.phase("spatial"):
            timings.note("features", 1)
            timings.record("temp_table", 0.076)
            timings.record("overlap_query", 0.013)
        with timings.phase("land_use"):
            timings.note("intersection_rows", 133)

    summary = collected.summary()

    assert "features=1 temp_table=0.076s overlap_query=0.013s" in summary
    assert "land_use=" in summary
    assert "[intersection_rows=133]" in summary
    assert collected.total_seconds >= 0


def test_helpers_are_noops_without_an_active_collector():
    timings.note("key", "value")
    timings.record("step", 1.0)
    with timings.phase("orphan"):
        pass


def test_collector_is_scoped_to_its_block():
    with timings.collect() as first, timings.phase("a"):
        pass

    with timings.collect() as second, timings.phase("b"):
        pass

    assert "a=" in first.summary()
    assert "a=" not in second.summary()
    assert "b=" in second.summary()
