"""Tests for the POST /compare-boundaries endpoint."""

import json
import logging
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from pyproj import Transformer

from app.main import app

ORIGIN_EASTING = 600_000
ORIGIN_NORTHING = 300_000
MIDDLEWARE_LOGGER = "app.common.tracing"
ROUTER_LOGGER = "app.boundary.router"

BNG_CRS = {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::27700"}}

UNKNOWN_KEY = "[[-1.234567,52.345678]]"


@pytest.fixture
def client():
    return TestClient(app)


def _bng_rectangle(width=100, height=100, shift_east=0):
    west = ORIGIN_EASTING + shift_east
    south = ORIGIN_NORTHING
    return {
        "type": "Polygon",
        "crs": BNG_CRS,
        "coordinates": [
            [
                [west, south],
                [west + width, south],
                [west + width, south + height],
                [west, south + height],
                [west, south],
            ]
        ],
    }


def _to_wgs84(polygon):
    transformer = Transformer.from_crs(27700, 4326, always_xy=True)
    return {
        "type": "Polygon",
        "coordinates": [
            [list(transformer.transform(x, y)) for x, y in polygon["coordinates"][0]]
        ],
    }


def _compare(client, original, new):
    return client.post(
        "/compare-boundaries",
        json={"boundaryOriginal": original, "boundaryNew": new},
    )


class TestBoundariesMatch:
    @pytest.mark.parametrize(
        ("new", "expected"),
        [
            pytest.param(_bng_rectangle(), True, id="identical"),
            pytest.param(_bng_rectangle(height=95), True, id="exactly-95-percent"),
            pytest.param(_bng_rectangle(height=105), True, id="grows-5-percent"),
            pytest.param(_bng_rectangle(height=106), False, id="grows-6-percent"),
            pytest.param(_bng_rectangle(height=94), False, id="shrinks-6-percent"),
            pytest.param(_bng_rectangle(shift_east=2), True, id="shifts-2-metres"),
            pytest.param(_bng_rectangle(shift_east=3), False, id="shifts-3-metres"),
            pytest.param(_bng_rectangle(shift_east=500), False, id="no-overlap"),
        ],
    )
    def test_matches_at_95_percent_either_way_round(self, client, new, expected):
        original = _bng_rectangle()

        forwards = _compare(client, original, new)
        backwards = _compare(client, new, original)

        assert forwards.status_code == 200
        assert forwards.json() == {"boundariesMatch": expected}
        assert backwards.json() == {"boundariesMatch": expected}

    def test_ignores_vertex_order_and_winding(self, client):
        original = _bng_rectangle()
        ring = original["coordinates"][0]
        reversed_from_another_corner = [*reversed(ring[:-1]), ring[-2]]
        new = {**original, "coordinates": [reversed_from_another_corner]}

        assert _compare(client, original, new).json() == {"boundariesMatch": True}

    def test_compares_a_wgs84_boundary_with_a_bng_boundary(self, client):
        original = _bng_rectangle()

        response = _compare(client, original, _to_wgs84(original))

        assert response.json() == {"boundariesMatch": True}

    @pytest.mark.parametrize(
        "wrap",
        [
            pytest.param(lambda g: g, id="bare-geometry"),
            pytest.param(
                lambda g: {"type": "Feature", "properties": {}, "geometry": g},
                id="feature",
            ),
            pytest.param(
                lambda g: {
                    "type": "FeatureCollection",
                    "features": [{"type": "Feature", "properties": {}, "geometry": g}],
                },
                id="feature-collection",
            ),
        ],
    )
    def test_accepts_each_geojson_wrapper(self, client, wrap):
        wgs84 = _to_wgs84(_bng_rectangle())

        response = _compare(client, wrap(wgs84), wrap(wgs84))

        assert response.json() == {"boundariesMatch": True}


def _wgs84_square():
    return _to_wgs84(_bng_rectangle())


def _feature_collection(*geometries):
    return {
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature", "properties": {}, "geometry": geometry}
            for geometry in geometries
        ],
    }


UNREADABLE = {"type": "Nonsense"}
EMPTY_POLYGON = {"type": "Polygon", "coordinates": []}
EMPTY_COLLECTION = _feature_collection()
NULL_GEOMETRY = {"type": "Feature", "properties": {}, "geometry": None}
TWO_POLYGONS = _feature_collection(_wgs84_square(), _wgs84_square())
POINT = {"type": "Point", "coordinates": [1.0, 52.0]}
UNCLOSED = {
    "type": "Polygon",
    "coordinates": [[[1.0, 52.0], [1.001, 52.0], [1.001, 52.001], [1.0, 52.001]]],
}
BOWTIE = {
    "type": "Polygon",
    "coordinates": [
        [[1.0, 52.0], [1.001, 52.001], [1.001, 52.0], [1.0, 52.001], [1.0, 52.0]]
    ],
}
WITH_HOLE = {
    "type": "Polygon",
    "coordinates": [
        [[1.0, 52.0], [1.01, 52.0], [1.01, 52.01], [1.0, 52.01], [1.0, 52.0]],
        [
            [1.002, 52.002],
            [1.003, 52.002],
            [1.003, 52.003],
            [1.002, 52.003],
            [1.002, 52.002],
        ],
    ],
}
DUPLICATE_VERTEX = {
    "type": "Polygon",
    "coordinates": [
        [
            [1.0, 52.0],
            [1.001, 52.0],
            [1.001, 52.0],
            [1.001, 52.001],
            [1.0, 52.001],
            [1.0, 52.0],
        ]
    ],
}
LONGITUDE_OUT_OF_RANGE = {
    "type": "Polygon",
    "coordinates": [[[200.0, 52.0], [201.0, 52.0], [201.0, 53.0], [200.0, 52.0]]],
}
OUTSIDE_GREAT_BRITAIN = {
    "type": "Polygon",
    "coordinates": [[[170.0, 10.0], [170.1, 10.0], [170.1, 10.1], [170.0, 10.0]]],
}
UNSUPPORTED_CRS = {
    **_wgs84_square(),
    "crs": {"type": "name", "properties": {"name": "EPSG:3857"}},
}
UNRESOLVABLE_CRS = {
    **_wgs84_square(),
    "crs": {"type": "name", "properties": {"name": "EPSG:999999"}},
}
COORDINATES_NOT_AN_ARRAY = {"type": "Polygon", "coordinates": 1}
FEATURES_NOT_AN_ARRAY = {"type": "FeatureCollection", "features": 1}
GEOMETRIES_NOT_AN_ARRAY = {"type": "GeometryCollection", "geometries": 1}
FEATURE_COORDINATES_NOT_AN_ARRAY = {
    "type": "Feature",
    "properties": {},
    "geometry": COORDINATES_NOT_AN_ARRAY,
}


class TestRejectedBoundaries:
    @pytest.mark.parametrize(
        ("boundary", "status", "code"),
        [
            pytest.param(UNREADABLE, 400, "unreadable_geometry_file", id="unreadable"),
            pytest.param(
                COORDINATES_NOT_AN_ARRAY,
                400,
                "unreadable_geometry_file",
                id="coordinates-not-an-array",
            ),
            pytest.param(
                FEATURES_NOT_AN_ARRAY,
                400,
                "unreadable_geometry_file",
                id="features-not-an-array",
            ),
            pytest.param(
                GEOMETRIES_NOT_AN_ARRAY,
                400,
                "unreadable_geometry_file",
                id="geometries-not-an-array",
            ),
            pytest.param(
                FEATURE_COORDINATES_NOT_AN_ARRAY,
                400,
                "unreadable_geometry_file",
                id="feature-coordinates-not-an-array",
            ),
            pytest.param(EMPTY_POLYGON, 400, "no_polygon_found", id="empty-polygon"),
            pytest.param(EMPTY_COLLECTION, 400, "no_polygon_found", id="no-features"),
            pytest.param(NULL_GEOMETRY, 400, "invalid_geometry", id="null-geometry"),
            pytest.param(TWO_POLYGONS, 400, "multiple_features", id="two-polygons"),
            pytest.param(POINT, 400, "unsupported_geometry_type", id="point"),
            pytest.param(UNCLOSED, 400, "unclosed_ring", id="unclosed"),
            pytest.param(BOWTIE, 400, "self_intersecting_geometry", id="bowtie"),
            pytest.param(WITH_HOLE, 400, "geometry_has_holes", id="hole"),
            pytest.param(DUPLICATE_VERTEX, 400, "duplicate_vertices", id="duplicate"),
            pytest.param(
                LONGITUDE_OUT_OF_RANGE,
                400,
                "coordinates_out_of_range",
                id="longitude-out-of-range",
            ),
            pytest.param(
                OUTSIDE_GREAT_BRITAIN,
                400,
                "coordinates_out_of_range",
                id="outside-great-britain",
            ),
            pytest.param(UNSUPPORTED_CRS, 422, "unsupported_crs", id="unsupported-crs"),
            pytest.param(
                UNRESOLVABLE_CRS, 422, "unsupported_crs", id="unresolvable-crs"
            ),
        ],
    )
    def test_names_the_problem_and_the_boundary(self, client, boundary, status, code):
        response = _compare(client, _wgs84_square(), boundary)

        assert response.status_code == status
        detail = response.json()["detail"]
        assert detail["error"] == code
        assert detail["boundary"] == "boundaryNew"
        assert detail["message"].startswith("boundaryNew ")

    def test_reports_the_original_boundary_when_it_is_the_bad_one(self, client):
        response = _compare(client, BOWTIE, _wgs84_square())

        detail = response.json()["detail"]
        assert detail["error"] == "self_intersecting_geometry"
        assert detail["boundary"] == "boundaryOriginal"
        assert detail["message"].startswith("boundaryOriginal ")


class TestMalformedRequests:
    @pytest.mark.parametrize(
        ("body", "named_field"),
        [
            pytest.param(
                {"boundaryOriginal": _wgs84_square()}, "boundaryNew", id="missing"
            ),
            pytest.param(
                {"boundaryOriginal": _wgs84_square(), "boundaryNew": "square"},
                "boundaryNew",
                id="not-an-object",
            ),
            pytest.param(
                {"boundary_original": _wgs84_square(), "boundaryNew": _wgs84_square()},
                "boundaryOriginal",
                id="snake-case-key",
            ),
        ],
    )
    def test_names_the_field_at_fault(self, client, body, named_field):
        response = client.post("/compare-boundaries", json=body)

        assert response.status_code == 400
        detail = response.json()["detail"]
        assert detail["error"] == "invalid_request"
        assert detail["boundary"] is None
        assert named_field in detail["message"]

    def test_rejects_an_unknown_key_without_repeating_it(self, client, caplog):
        body = {
            "boundaryOriginal": _wgs84_square(),
            "boundaryNew": _wgs84_square(),
            UNKNOWN_KEY: 1,
        }

        with caplog.at_level(logging.DEBUG):
            response = client.post("/compare-boundaries", json=body)

        assert response.status_code == 400
        assert response.json()["detail"]["error"] == "invalid_request"
        assert UNKNOWN_KEY not in response.text
        assert UNKNOWN_KEY not in caplog.text

    def test_reports_many_unknown_keys_as_one_fault(self, client):
        body = {
            "boundaryOriginal": _wgs84_square(),
            "boundaryNew": _wgs84_square(),
            **{f"unknown{index}": 1 for index in range(3)},
        }

        response = client.post("/compare-boundaries", json=body)

        assert ";" not in response.json()["detail"]["message"]

    def test_rejects_a_body_that_is_not_json(self, client):
        response = client.post("/compare-boundaries", content=b"not json")

        assert response.status_code == 400
        assert response.json()["detail"]["error"] == "invalid_request"

    def test_does_not_echo_coordinates_back(self, client):
        response = client.post(
            "/compare-boundaries", json={"boundaryOriginal": _bng_rectangle()}
        )

        assert str(ORIGIN_EASTING) not in response.text

    @pytest.mark.parametrize(
        ("spare_bytes", "expected_status"),
        [pytest.param(0, 200, id="at-limit"), pytest.param(-1, 413, id="over-limit")],
    )
    def test_limits_the_body_size(self, client, spare_bytes, expected_status):
        body = json.dumps(
            {"boundaryOriginal": _bng_rectangle(), "boundaryNew": _bng_rectangle()}
        ).encode()

        with patch(
            "app.boundary.router._max_compare_body_bytes", len(body) + spare_bytes
        ):
            response = client.post("/compare-boundaries", content=body)

        assert response.status_code == expected_status


class TestLogging:
    def test_logs_the_score_the_response_leaves_out(self, client, caplog):
        with caplog.at_level(logging.INFO, logger=ROUTER_LOGGER):
            _compare(client, _bng_rectangle(), _bng_rectangle(height=106))

        messages = [
            record.getMessage()
            for record in caplog.records
            if record.name == ROUTER_LOGGER
        ]
        assert len(messages) == 1
        assert "match=False" in messages[0]
        assert "overlap_ratio=0.9434" in messages[0]
        assert "original_area_ha=1.0000" in messages[0]
        assert "new_area_ha=1.0600" in messages[0]

    def test_logs_each_rejection_once_with_its_code(self, client, caplog):
        with caplog.at_level(logging.INFO):
            _compare(client, _wgs84_square(), BOWTIE)

        rejections = [
            record for record in caplog.records if record.levelno >= logging.WARNING
        ]
        assert [record.name for record in rejections] == [MIDDLEWARE_LOGGER]
        assert "self_intersecting_geometry" in rejections[0].getMessage()
        assert "boundaryNew" in rejections[0].getMessage()

    @pytest.mark.parametrize(
        "new",
        [
            pytest.param(_bng_rectangle(), id="compared"),
            pytest.param({**BOWTIE, "crs": BNG_CRS}, id="rejected"),
        ],
    )
    def test_never_logs_coordinates(self, client, caplog, new):
        with caplog.at_level(logging.DEBUG):
            _compare(client, _bng_rectangle(), new)

        assert str(ORIGIN_EASTING) not in caplog.text
