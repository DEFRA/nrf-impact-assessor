"""Regression tests for Impact Assessment Worker.

These tests validate that PostGIS-based implementation produces identical results
to the legacy file-based implementation.

Prerequisites:
- PostgreSQL with PostGIS running (docker compose up -d)
- Full reference data loaded into nrf_impact database
- Run: uv run python scripts/load_data.py

Usage:
    uv run pytest -m regression
"""
