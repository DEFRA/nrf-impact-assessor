"""Shared test utilities for unit/api tests."""

import json
from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def _no_excluded_areas():
    """Default every endpoint test to "no exclusion zones hit".

    /check-boundary queries exclusion zones before EDP areas, so without this
    each test that gets past validation would fall through to a real PostGIS
    connection. Exclusion-specific tests override it with their own @patch,
    which takes precedence over this fixture.
    """
    with patch(
        "app.boundary.router._find_intersecting_excluded_areas", return_value=[]
    ):
        yield


@pytest.fixture(autouse=True)
def _no_catchments():
    """Default every endpoint test to "no NN catchments hit".

    /check-boundary queries catchments whenever an EDP is returned, so without
    this each test that mocks a non-empty EDP result would fall through to a
    real PostGIS connection. Catchment-specific tests override it with their
    own @patch, which takes precedence over this fixture.
    """
    with patch("app.boundary.router._find_intersecting_catchments", return_value=[]):
        yield


def _make_geojson_bytes(
    coordinates: list | None = None,
    crs: str | None = None,
) -> bytes:
    """Create a minimal GeoJSON FeatureCollection as bytes."""
    if coordinates is None:
        coordinates = [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]

    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": coordinates,
                },
                "properties": {"name": "test"},
            }
        ],
    }
    if crs:
        geojson["crs"] = {
            "type": "name",
            "properties": {"name": crs},
        }
    return json.dumps(geojson).encode()
