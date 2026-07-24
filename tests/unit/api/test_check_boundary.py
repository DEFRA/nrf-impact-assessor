"""Tests for the POST /check-boundary endpoint."""

import json
import tempfile
import zipfile
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import geopandas as gpd
import pytest
from fastapi.testclient import TestClient
from shapely.geometry import Polygon

from app.boundary.router import _find_intersecting_edps
from app.main import app
from tests.unit.api.conftest import _make_geojson_bytes


@pytest.fixture
def client():
    return TestClient(app)


def _mock_no_edp_intersections(gdf, repository, output_srid=4326):
    """Mock that returns no intersecting EDPs."""
    return []


def _mock_edp_intersections(gdf, repository, output_srid=4326):
    """Mock that returns intersecting EDPs."""
    return [
        {
            "label": "Norfolk EDP 1",
            "n2k_site_name": "Site A",
            "edp_geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-1.6, 51.9],
                        [-1.3, 51.9],
                        [-1.3, 52.2],
                        [-1.6, 52.2],
                        [-1.6, 51.9],
                    ]
                ],
            },
            "intersection_geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-1.5, 52.0],
                        [-1.4, 52.0],
                        [-1.4, 52.1],
                        [-1.5, 52.1],
                        [-1.5, 52.0],
                    ]
                ],
            },
            "overlap_area_ha": 0.5,
            "overlap_area_sqm": 5000.0,
            "overlap_percentage": 25.0,
        },
        {
            "label": "Norfolk EDP 2",
            "n2k_site_name": "Site B",
            "edp_geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-1.4, 51.9],
                        [-1.1, 51.9],
                        [-1.1, 52.2],
                        [-1.4, 52.2],
                        [-1.4, 51.9],
                    ]
                ],
            },
            "intersection_geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-1.3, 52.0],
                        [-1.2, 52.0],
                        [-1.2, 52.1],
                        [-1.3, 52.1],
                        [-1.3, 52.0],
                    ]
                ],
            },
            "overlap_area_ha": 0.3,
            "overlap_area_sqm": 3000.0,
            "overlap_percentage": 15.0,
        },
    ]


def _post_boundary(client, filename, content, content_type="application/json"):
    """Post a file to the /check-boundary endpoint."""
    return client.post(
        "/check-boundary",
        files={"geometry_file": (filename, BytesIO(content), content_type)},
    )


def _post_boundary_file(
    client, filename, file_buf, content_type, *, boundary_filename=None
):
    """Post a file buffer to the /check-boundary endpoint."""
    data = (
        {"boundary_filename": boundary_filename}
        if boundary_filename is not None
        else None
    )
    return client.post(
        "/check-boundary",
        files={"geometry_file": (filename, file_buf, content_type)},
        data=data,
    )


def _make_shapefile_zip_without_crs():
    """Create a zip containing a shapefile with no .prj (no CRS)."""
    gdf = gpd.GeoDataFrame(
        {"id": [1]},
        geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
        crs=None,
    )
    with tempfile.TemporaryDirectory() as tmpdir:
        shp_path = Path(tmpdir) / "no_crs.shp"
        gdf.to_file(shp_path)
        for prj in Path(tmpdir).glob("*.prj"):
            prj.unlink()

        zip_buf = BytesIO()
        with zipfile.ZipFile(zip_buf, "w") as zf:
            for f in Path(tmpdir).glob("no_crs.*"):
                zf.write(f, f.name)
        zip_buf.seek(0)
    return zip_buf


def _make_shapefile_zip_with_crs(crs: str):
    """Create a zip containing a shapefile with the given CRS."""
    gdf = gpd.GeoDataFrame(
        {"id": [1]},
        geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
        crs=crs,
    )
    with tempfile.TemporaryDirectory() as tmpdir:
        shp_path = Path(tmpdir) / "boundary.shp"
        gdf.to_file(shp_path)

        zip_buf = BytesIO()
        with zipfile.ZipFile(zip_buf, "w") as zf:
            for f in Path(tmpdir).glob("boundary.*"):
                zf.write(f, f.name)
        zip_buf.seek(0)
    return zip_buf


def _make_multi_shapefile_zip() -> BytesIO:
    """Create a zip containing two distinct complete shapefile bundles."""
    gdf_a = gpd.GeoDataFrame(
        {"id": [1]},
        geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
        crs="EPSG:4326",
    )
    gdf_b = gpd.GeoDataFrame(
        {"id": [2]},
        geometry=[Polygon([(10, 10), (11, 10), (11, 11), (10, 11)])],
        crs="EPSG:4326",
    )
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        gdf_a.to_file(tmp / "alpha.shp")
        gdf_b.to_file(tmp / "zebra.shp")

        zip_buf = BytesIO()
        with zipfile.ZipFile(zip_buf, "w") as zf:
            for pattern in ("alpha.*", "zebra.*"):
                for f in tmp.glob(pattern):
                    zf.write(f, f.name)
        zip_buf.seek(0)
    return zip_buf


class TestCheckBoundaryGeoJSON:
    """Tests for POST /check-boundary with GeoJSON files."""

    @patch("app.boundary.router._find_intersecting_edps", _mock_no_edp_intersections)
    def test_valid_geojson_returns_polygon_geometry(self, client):
        response = _post_boundary(client, "boundary.geojson", _make_geojson_bytes())

        assert response.status_code == 200
        body = response.json()
        assert body["boundaryGeometryWgs84"]["type"] == "Polygon"
        assert len(body["boundaryGeometryWgs84"]["coordinates"]) >= 1

    @patch("app.boundary.router._find_intersecting_edps", _mock_no_edp_intersections)
    def test_properties_are_not_included(self, client):
        """User-supplied properties should not be present in bare geometry output."""
        response = _post_boundary(client, "boundary.geojson", _make_geojson_bytes())

        assert response.status_code == 200
        geom = response.json()["boundaryGeometryWgs84"]
        assert set(geom.keys()) == {"type", "coordinates"}

    @patch("app.boundary.router._find_intersecting_edps", _mock_no_edp_intersections)
    def test_json_extension_accepted(self, client):
        response = _post_boundary(client, "boundary.json", _make_geojson_bytes())

        assert response.status_code == 200
        assert response.json()["boundaryGeometryWgs84"]["type"] == "Polygon"

    @patch("app.boundary.router._find_intersecting_edps", _mock_no_edp_intersections)
    def test_multiple_features_returns_first_polygon(self, client):
        """When input has multiple features, only the first polygon is returned."""
        geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
                    },
                    "properties": {"id": 1},
                },
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[2, 2], [3, 2], [3, 3], [2, 3], [2, 2]]],
                    },
                    "properties": {"id": 2},
                },
            ],
        }
        content = json.dumps(geojson).encode()
        response = _post_boundary(client, "multi.geojson", content)

        assert response.status_code == 200
        body = response.json()
        assert body["boundaryGeometryWgs84"]["type"] == "Polygon"
        assert body["boundaryGeometryOriginal"]["type"] == "Polygon"

    def test_invalid_geojson_returns_400(self, client):
        response = _post_boundary(client, "bad.geojson", b"not valid json")

        assert response.status_code == 400
        assert response.json()["error"] == "unreadable_geometry_file"

    def test_corrupt_geometry_returns_400(self, client):
        """Valid JSON with malformed/incomplete coordinates must return 400, not 500."""
        corrupt = json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {},
                        "geometry": {"type": "Polygon"},
                    }
                ],
            }
        ).encode()
        response = _post_boundary(client, "corrupt.geojson", corrupt)

        assert response.status_code == 400
        assert response.json()["error"] == "invalid_geometry"

    def test_unsupported_format_returns_400(self, client):
        response = _post_boundary(
            client, "data.csv", b"col1,col2\n1,2", content_type="text/csv"
        )

        assert response.status_code == 400
        assert response.json()["error"] == "unsupported_file_type"

    def test_file_too_large_returns_413(self, client):
        from app.boundary.router import _max_upload_bytes

        content = b"x" * (_max_upload_bytes + 1)
        response = _post_boundary(client, "huge.geojson", content)

        assert response.status_code == 413
        assert response.json()["error"] == "file_size_too_large"

    def test_shapefile_without_crs_returns_422(self, client):
        """A .shp without a .prj has no CRS — should return 422."""
        zip_buf = _make_shapefile_zip_without_crs()
        response = _post_boundary_file(client, "no_crs.zip", zip_buf, "application/zip")

        assert response.status_code == 422
        assert response.json()["error"] == "missing_crs"

    def test_shapefile_zip_missing_companion_files_returns_400(self, client):
        """A zip with only .shp (no .dbf/.shx) should return 400."""
        zip_buf = BytesIO()
        with zipfile.ZipFile(zip_buf, "w") as zf:
            zf.writestr("boundary.shp", b"fake shapefile content")
        zip_buf.seek(0)

        response = _post_boundary_file(
            client, "incomplete.zip", zip_buf, "application/zip"
        )

        assert response.status_code == 400
        assert response.json()["error"] == "zip_missing_shapefile_parts"

    @patch("app.boundary.router._find_intersecting_edps", _mock_no_edp_intersections)
    def test_multi_shapefile_zip_uses_explicit_boundary_filename(self, client):
        """When the caller specifies a boundary_filename, that .shp is used."""
        zip_buf = _make_multi_shapefile_zip()

        response = _post_boundary_file(
            client,
            "multi.zip",
            zip_buf,
            "application/zip",
            boundary_filename="zebra.shp",
        )

        assert response.status_code == 200
        geom = response.json()["boundaryGeometryWgs84"]
        # zebra.shp was written with coordinates around (10, 10)-(11, 11);
        # alpha.shp used (0, 0)-(1, 1). Confirm we loaded zebra's geometry
        # (tolerance covers round-trip reprojection artefacts).
        xs = [pt[0] for pt in geom["coordinates"][0]]
        assert min(xs) > 9

    def test_multi_shapefile_zip_rejects_unknown_boundary_filename(self, client):
        """An unknown boundary_filename should 400 rather than fall back."""
        zip_buf = _make_multi_shapefile_zip()

        response = _post_boundary_file(
            client,
            "multi.zip",
            zip_buf,
            "application/zip",
            boundary_filename="no-such-file.shp",
        )

        assert response.status_code == 400
        assert response.json()["error"] == "boundary_file_not_found_in_zip"


class TestCheckBoundaryGeometryValidation:
    """Tests for geometry validation in POST /check-boundary."""

    def test_self_intersecting_polygon_returns_400_with_geometry(self, client):
        """A bowtie/figure-of-8 polygon should be rejected but include parsed geometry."""
        content = _make_geojson_bytes(
            coordinates=[[[0, 0], [1, 1], [1, 0], [0, 1], [0, 0]]]
        )
        response = _post_boundary(client, "self-intersecting.geojson", content)

        assert response.status_code == 400
        body = response.json()
        assert body["error"] == "self_intersecting_geometry"
        assert body["boundaryGeometryWgs84"]["type"] == "FeatureCollection"
        assert len(body["boundaryGeometryWgs84"]["features"]) == 1
        # Metadata (bounds/centre) is included so the frontend can still zoom
        # the map to the invalid boundary.
        metadata = body["boundaryMetadata"]
        assert metadata["bounds"] is not None
        # The centre must sit within the bounds. A self-intersecting polygon's
        # centroid can fall outside the shape, which centred the map on the
        # wrong area; the bounding-box midpoint always sits inside the bounds.
        bounds = metadata["bounds"]
        min_lng = bounds["bottomLeft"][0]
        max_lng = bounds["topRight"][0]
        min_lat = bounds["bottomLeft"][1]
        max_lat = bounds["topRight"][1]
        centre_lng, centre_lat = metadata["centre"]
        assert min_lng <= centre_lng <= max_lng
        assert min_lat <= centre_lat <= max_lat

    @patch("app.boundary.router._find_intersecting_edps", _mock_no_edp_intersections)
    def test_valid_polygon_passes_validation(self, client):
        """A valid polygon should pass geometry validation."""
        content = _make_geojson_bytes(
            coordinates=[[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]
        )
        response = _post_boundary(client, "valid.geojson", content)

        assert response.status_code == 200

    def test_polygon_with_holes_returns_400(self, client):
        """A polygon with interior rings (holes) should be rejected."""
        content = _make_geojson_bytes(
            coordinates=[
                [[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]],
                [[2, 2], [2, 4], [4, 4], [4, 2], [2, 2]],
            ]
        )
        response = _post_boundary(client, "holes.geojson", content)

        assert response.status_code == 400
        body = response.json()
        assert body["error"] == "geometry_has_holes"

    def test_duplicate_consecutive_vertices_returns_400(self, client):
        """A polygon with duplicate consecutive vertices should be rejected."""
        content = _make_geojson_bytes(
            coordinates=[[[0, 0], [1, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]
        )
        response = _post_boundary(client, "duplicates.geojson", content)

        assert response.status_code == 400
        body = response.json()
        assert body["error"] == "duplicate_vertices"

    def test_missing_crs_returns_422(self, client):
        """A shapefile with no CRS defined should return a missing_crs code."""
        zip_buf = _make_shapefile_zip_without_crs()
        response = _post_boundary_file(client, "no_crs.zip", zip_buf, "application/zip")

        assert response.status_code == 422
        assert response.json()["error"] == "missing_crs"


class TestCheckBoundaryUnsupportedCRS:
    """Tests for unsupported coordinate reference systems."""

    def test_unsupported_crs_in_geojson_returns_422(self, client):
        """GeoJSON with an unsupported CRS (e.g. Web Mercator) should be rejected."""
        content = _make_geojson_bytes(
            coordinates=[
                [
                    [0, 0],
                    [100000, 0],
                    [100000, 100000],
                    [0, 100000],
                    [0, 0],
                ]
            ],
            crs="urn:ogc:def:crs:EPSG::3857",
        )
        response = _post_boundary(client, "mercator.geojson", content)

        assert response.status_code == 422
        assert response.json()["error"] == "unsupported_crs"

    def test_unresolvable_crs_in_geojson_returns_422(self, client):
        """GeoJSON declaring a CRS that doesn't exist (e.g. a made-up EPSG
        code) should be rejected as unsupported, not silently treated as
        WGS84 and rejected by an unrelated geometry check."""
        content = _make_geojson_bytes(crs="urn:ogc:def:crs:EPSG::99999")
        response = _post_boundary(client, "unknown_crs.geojson", content)

        assert response.status_code == 422
        assert response.json()["error"] == "unsupported_crs"

    def test_unsupported_crs_in_shapefile_returns_422(self, client):
        """Shapefile with an unsupported CRS should be rejected."""
        zip_buf = _make_shapefile_zip_with_crs("EPSG:3857")
        response = _post_boundary_file(
            client, "mercator.zip", zip_buf, "application/zip"
        )

        assert response.status_code == 422
        assert response.json()["error"] == "unsupported_crs"

    def test_unrecognised_crs_in_shapefile_returns_422(self, client):
        """Shapefile with a corrupted/unrecognised .prj should return 422."""
        gdf = gpd.GeoDataFrame(
            {"id": [1]},
            geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
            crs="EPSG:27700",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            shp_path = Path(tmpdir) / "bad_crs.shp"
            gdf.to_file(shp_path)
            prj_path = Path(tmpdir) / "bad_crs.prj"
            prj_path.write_text("GARBAGE_NOT_A_REAL_CRS")

            zip_buf = BytesIO()
            with zipfile.ZipFile(zip_buf, "w") as zf:
                for f in Path(tmpdir).glob("bad_crs.*"):
                    zf.write(f, f.name)
            zip_buf.seek(0)

        response = _post_boundary_file(
            client, "bad_crs.zip", zip_buf, "application/zip"
        )

        assert response.status_code in (400, 422)
        error = response.json()["error"]
        assert (
            "coordinate" in error.lower()
            or "crs" in error.lower()
            or "read" in error.lower()
        )


_BNG_COORDINATES = [
    [
        [400000, 100000],
        [400100, 100000],
        [400100, 100100],
        [400000, 100100],
        [400000, 100000],
    ]
]


class TestCheckBoundaryProjection:
    """Tests for output projection."""

    @patch("app.boundary.router._find_intersecting_edps", _mock_no_edp_intersections)
    def test_bng_input_reprojected_to_wgs84(self, client):
        """BNG input geometry should be reprojected to WGS84 in boundaryGeometryWgs84."""
        content = _make_geojson_bytes(
            coordinates=_BNG_COORDINATES,
            crs="urn:ogc:def:crs:EPSG::27700",
        )
        response = _post_boundary(client, "boundary.geojson", content)

        assert response.status_code == 200
        body = response.json()
        coords = body["boundaryGeometryWgs84"]["coordinates"][0]
        for lng, lat in coords:
            assert -180 <= lng <= 180, f"longitude {lng} out of WGS84 range"
            assert -90 <= lat <= 90, f"latitude {lat} out of WGS84 range"

    @patch("app.boundary.router._find_intersecting_edps", _mock_no_edp_intersections)
    def test_original_geometry_preserves_input_crs(self, client):
        """boundaryGeometryOriginal should keep the input CRS (BNG)."""
        content = _make_geojson_bytes(
            coordinates=_BNG_COORDINATES,
            crs="urn:ogc:def:crs:EPSG::27700",
        )
        response = _post_boundary(client, "boundary.geojson", content)

        assert response.status_code == 200
        body = response.json()
        original = body["boundaryGeometryOriginal"]
        assert original["type"] == "Polygon"
        coords = original["coordinates"][0]
        for e, n in coords:
            assert abs(e) > 180 or abs(n) > 180, "Expected BNG coordinates"
        assert original["crs"]["type"] == "name"
        assert "27700" in original["crs"]["properties"]["name"]


class TestCheckBoundaryEdpIntersection:
    """Tests for EDP intersection logic in the response."""

    @patch("app.boundary.router._find_intersecting_edps", _mock_no_edp_intersections)
    def test_no_intersections_returns_empty_list(self, client):
        response = _post_boundary(client, "boundary.geojson", _make_geojson_bytes())

        assert response.status_code == 200
        body = response.json()
        assert body["intersectingEdps"] == []

    @patch("app.boundary.router._find_intersecting_edps", _mock_edp_intersections)
    def test_intersections_returns_edp_details(self, client):
        response = _post_boundary(client, "boundary.geojson", _make_geojson_bytes())

        assert response.status_code == 200
        body = response.json()
        assert len(body["intersectingEdps"]) == 2
        assert body["intersectingEdps"][0]["label"] == "Norfolk EDP 1"
        assert body["intersectingEdps"][1]["label"] == "Norfolk EDP 2"
        assert body["intersectingEdps"][0]["overlap_area_ha"] == pytest.approx(0.5)
        assert body["intersectingEdps"][0]["overlap_percentage"] == pytest.approx(25.0)
        assert body["intersectingEdps"][0]["intersection_geometry"]["type"] == "Polygon"
        assert body["intersectingEdps"][0]["edp_geometry"]["type"] == "Polygon"

    @patch("app.boundary.router._find_intersecting_edps", _mock_edp_intersections)
    def test_response_contains_all_expected_keys(self, client):
        response = _post_boundary(client, "boundary.geojson", _make_geojson_bytes())

        assert response.status_code == 200
        body = response.json()
        assert set(body.keys()) == {
            "boundaryGeometryOriginal",
            "boundaryGeometryWgs84",
            "intersectingEdps",
            "boundaryMetadata",
            "error",
        }
        assert body["error"] is None
        assert body["boundaryMetadata"] is not None
        assert set(body["boundaryMetadata"].keys()) == {
            "area",
            "perimeter",
            "centre",
            "bounds",
        }


class TestFindIntersectingEdpsMapping:
    """Regression tests for the row -> dict mapping in _find_intersecting_edps.

    The other tests mock the whole function, so they cannot catch a schema
    drift in the EDP boundary attribute keys. These exercise the real mapping
    against a stubbed repository session.
    """

    def _make_row(self, attributes):
        return SimpleNamespace(
            attributes=attributes,
            edp_geojson=json.dumps({"type": "Polygon", "coordinates": []}),
            intersection_geojson=json.dumps({"type": "Polygon", "coordinates": []}),
            intersection_area_sqm=5000.0,
        )

    def _run(self, rows):
        session = MagicMock()
        session.execute.return_value.fetchall.return_value = rows
        repository = MagicMock()
        repository.session.return_value.__enter__.return_value = session

        gdf = gpd.GeoDataFrame(
            geometry=[Polygon([(0, 0), (0, 100), (100, 100), (100, 0)])],
            crs="EPSG:27700",
        )
        return _find_intersecting_edps(gdf, repository)

    def test_label_and_n2k_site_name_come_from_edp_name(self):
        rows = [
            self._make_row(
                {
                    "OBJECTID": 2,
                    "EDP_Area": "Norfolk",
                    "EDP_Name": "Broads SAC (Yare & Bure) & Wensum SAC",
                }
            )
        ]

        results = self._run(rows)

        assert len(results) == 1
        assert results[0]["label"] == "Broads SAC (Yare & Bure) & Wensum SAC"
        assert results[0]["n2k_site_name"] == "Broads SAC (Yare & Bure) & Wensum SAC"

    def test_missing_attributes_map_to_none(self):
        results = self._run([self._make_row(None)])

        assert results[0]["label"] is None
        assert results[0]["n2k_site_name"] is None
