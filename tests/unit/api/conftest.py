"""Shared test utilities for unit/api tests."""

import json


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
