"""Boundary checking endpoint.

Accepts a geometry file (.geojson, .kml, or .zip containing shapefile components)
and checks whether the uploaded geometry intersects with EDP areas.
"""

import json
import logging
import tempfile
import zipfile
from io import BytesIO
from pathlib import Path
from typing import Annotated

import geopandas as gpd
from fastapi import APIRouter, Form, UploadFile
from fastapi.responses import JSONResponse
from geoalchemy2.functions import (
    ST_Area,
    ST_AsGeoJSON,
    ST_CollectionExtract,
    ST_GeomFromText,
    ST_Intersection,
    ST_Intersects,
    ST_SetSRID,
    ST_Transform,
)
from pyproj import CRS
from pyproj.exceptions import CRSError
from sqlalchemy import select

from app.boundary.validation import SUPPORTED_CRS, validate_geometry
from app.config import ApiServerConfig
from app.models.db import EdpBoundaryLayer
from app.repositories.engine import get_shared_repository
from app.repositories.repository import Repository
from app.spatial.utils import UnsupportedCRSError, ensure_crs

logger = logging.getLogger(__name__)

_VALID_GEOM_TYPES = {"Polygon"}
_WGS84 = "EPSG:4326"


router = APIRouter()

_config = ApiServerConfig()
_max_upload_bytes = _config.max_upload_bytes


def _get_repository() -> Repository:
    """Return the process-wide shared Repository."""
    return get_shared_repository()


_EXT_GEOJSON = ".geojson"
_EXT_JSON = ".json"
_EXT_KML = ".kml"
_EXT_ZIP = ".zip"
_GEOJSON_EXTENSIONS = frozenset({_EXT_GEOJSON, _EXT_JSON})
_WGS84_EXTENSIONS = frozenset({_EXT_GEOJSON, _EXT_JSON, _EXT_KML})
_SUPPORTED_EXTENSIONS = frozenset({_EXT_ZIP, _EXT_GEOJSON, _EXT_JSON, _EXT_KML})


def _compute_boundary_metadata(
    geom_projected,  # Shapely geometry in a metric CRS (e.g. BNG/EPSG:27700)
    geom_wgs84,  # Shapely geometry in WGS84
) -> dict:
    area_sqm = geom_projected.area
    perimeter_m = geom_projected.length
    minx, miny, maxx, maxy = geom_wgs84.bounds
    # Use the bounding-box midpoint rather than the polygon centroid: for
    # self-intersecting/invalid geometries the centroid can fall outside the
    # shape, which centres the map on the wrong area. The bbox midpoint is
    # always consistent with the bounds the map zooms to.
    return {
        "area": {
            "hectares": round(area_sqm / 10_000, 4),
            "acres": round(area_sqm / 4_046.856, 4),
        },
        "perimeter": {
            "kilometres": round(perimeter_m / 1_000, 4),
            "miles": round(perimeter_m / 1_609.344, 4),
        },
        "centre": [round((minx + maxx) / 2, 6), round((miny + maxy) / 2, 6)],
        "bounds": {
            "topLeft": [round(minx, 6), round(maxy, 6)],
            "topRight": [round(maxx, 6), round(maxy, 6)],
            "bottomRight": [round(maxx, 6), round(miny, 6)],
            "bottomLeft": [round(minx, 6), round(miny, 6)],
        },
    }


def _make_response(
    status_code: int = 200,
    *,
    boundary_geometry_original: dict | None = None,
    boundary_geometry_wgs84: dict | None = None,
    intersecting_edps: list | None = None,
    boundary_metadata: dict | None = None,
    error: str | None = None,
) -> JSONResponse:
    """Build a consistent JSON response for the check-boundary endpoint."""
    return JSONResponse(
        status_code=status_code,
        content={
            "boundaryGeometryOriginal": boundary_geometry_original,
            "boundaryGeometryWgs84": boundary_geometry_wgs84,
            "intersectingEdps": intersecting_edps or [],
            "boundaryMetadata": boundary_metadata,
            "error": error,
        },
    )


def _validate_extension(filename: str) -> str:
    """Validate the file extension and return it.

    Only used for control flow (comparisons), never in path construction.
    """
    suffix = Path(filename).suffix.lower()
    if suffix not in _SUPPORTED_EXTENSIONS:
        code = "unsupported_file_type"
        raise ValueError(code)
    return suffix


def _write_to_temp(content: bytes, tmpdir: Path, suffix: str) -> Path:
    """Write content to a system-generated temporary file.

    Uses tempfile.NamedTemporaryFile so the path is entirely OS-generated
    with no user-controlled data in the filename.
    """
    with tempfile.NamedTemporaryFile(dir=tmpdir, suffix=suffix, delete=False) as tmp:
        tmp.write(content)
        return Path(tmp.name)


def _check_declared_geojson_crs(content: bytes, ext: str) -> None:
    """Reject a GeoJSON/JSON file that declares an unresolvable CRS.

    GDAL honours a valid, resolvable "crs" member on read, but when the
    declared CRS name can't be resolved at all (e.g. an EPSG code that
    doesn't exist) it silently falls back to assuming WGS84 — as GeoJSON
    (RFC 7946) deprecated the "crs" member and mandates WGS84 by default —
    rather than surfacing an error. That leaves the file's real coordinates
    misread as WGS84, so it fails a later, unrelated validation check
    instead of a CRS one. We check the declared CRS ourselves before
    handing the file to geopandas, so an unresolvable CRS is rejected with
    the correct CRS error instead.

    Raises:
        UnsupportedCRSError: If a declared CRS is present but unrecognised
            or not in SUPPORTED_CRS.
    """
    if ext not in _GEOJSON_EXTENSIONS:
        return

    try:
        crs_name = json.loads(content)["crs"]["properties"]["name"]
    except (json.JSONDecodeError, UnicodeDecodeError, KeyError, TypeError):
        return

    try:
        epsg = CRS(crs_name).to_epsg()
    except CRSError as e:
        msg = f"Unrecognised coordinate reference system: {e}"
        raise UnsupportedCRSError(msg) from e

    # A resolvable CRS with no EPSG mapping (e.g. OGC:CRS84, the RFC 7946
    # canonical GeoJSON CRS) is left for GDAL/ensure_crs to normalise —
    # only a name that fails to resolve at all is rejected here.
    if epsg is not None and epsg not in SUPPORTED_CRS:
        msg = f"Unsupported coordinate reference system: EPSG:{epsg}"
        raise UnsupportedCRSError(msg)


def _read_geometry(
    content: bytes,
    filename: str,
    tmpdir: Path,
    boundary_filename: str | None = None,
) -> gpd.GeoDataFrame:
    """Read a geometry file from uploaded bytes into a GeoDataFrame.

    Supports .geojson, .json, .kml, and .zip (containing .shp, .geojson, or .kml).

    Args:
        content: Raw file bytes.
        filename: Original filename (used for extension detection).
        tmpdir: Temporary directory to write files into.
        boundary_filename: Bare filename (no directory) of the entry inside
            `filename` that should be used when `filename` is a zip — today
            always a .shp selected by the backend during zip validation, but
            the parameter is deliberately format-agnostic so future bundled
            formats can flow through the same contract. Ignored for non-zip
            uploads.

    Returns:
        GeoDataFrame with the uploaded geometries.

    Raises:
        HTTPException: If the file format is unsupported or unreadable.
    """
    ext = _validate_extension(filename)

    try:
        if ext in _GEOJSON_EXTENSIONS:
            return gpd.read_file(BytesIO(content))
        if ext == _EXT_KML:
            return gpd.read_file(BytesIO(content), driver="KML")
        if ext == _EXT_ZIP:
            zip_path = _write_to_temp(content, tmpdir, _EXT_ZIP)
            read_path = _extract_zip(zip_path, tmpdir, boundary_filename)
            return gpd.read_file(read_path)
    except ValueError:
        raise
    except Exception as e:
        code = "unreadable_geometry_file"
        raise ValueError(code) from e


def _extract_zip(
    zip_path: Path, tmpdir: Path, boundary_filename: str | None = None
) -> Path:
    """Extract a zip archive and return the path to the geometry file inside.

    If `boundary_filename` is supplied (the normal case when called from the
    backend), we locate that exact entry inside the extracted zip and use it.
    Otherwise we fall back to picking the first .shp / .geojson / .kml found
    by glob — this path is only exercised by direct callers of the IA that
    don't know which file to ask for.
    """
    extract_dir = tmpdir / "extracted"
    extract_dir.mkdir()
    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.infolist():
            member_path = (extract_dir / member.filename).resolve()
            if not member_path.is_relative_to(extract_dir.resolve()):
                code = "zip_unsafe_path"
                raise ValueError(code)
        zf.extractall(extract_dir)

    if boundary_filename:
        return _locate_named_entry(extract_dir, boundary_filename)

    shp_files = list(extract_dir.glob("**/*.shp"))
    geojson_files = list(extract_dir.glob("**/*.geojson"))
    kml_files = list(extract_dir.glob("**/*.kml"))

    if shp_files:
        return _check_shapefile_companions(shp_files[0])
    if geojson_files:
        return geojson_files[0]
    if kml_files:
        return kml_files[0]

    code = "zip_missing_shapefile"
    raise ValueError(code)


def _locate_named_entry(extract_dir: Path, boundary_filename: str) -> Path:
    """Find a specific file inside the extracted zip, matched by bare filename.

    We match on the filename only (not the full in-zip path) so the backend
    doesn't need to know whether entries were at the top level or nested in a
    subdirectory. Case-insensitive because zip tools on Windows/macOS routinely
    mangle extension casing.
    """
    lowered = boundary_filename.lower()
    candidates = [
        p for p in extract_dir.glob("**/*") if p.name.lower() == lowered and p.is_file()
    ]
    if not candidates:
        code = "boundary_file_not_found_in_zip"
        raise ValueError(code)
    # Multiple matches would mean the same filename appears in two different
    # subdirectories — ambiguous, so refuse rather than guess.
    if len(candidates) > 1:
        code = "zip_ambiguous_filename"
        raise ValueError(code)
    entry = candidates[0]
    # Shapefiles still need their sibling .dbf / .shx in the same directory.
    if entry.suffix.lower() == ".shp":
        return _check_shapefile_companions(entry)
    return entry


def _check_shapefile_companions(shp_path: Path) -> Path:
    """Verify a .shp has its required .dbf/.shx siblings in the same directory."""
    stem = shp_path.stem
    shp_dir = shp_path.parent
    missing = [
        ext for ext in (".dbf", ".shx") if not (shp_dir / f"{stem}{ext}").exists()
    ]
    if missing:
        code = "zip_missing_shapefile_parts"
        raise ValueError(code)
    return shp_path


def _find_intersecting_edps(
    gdf: gpd.GeoDataFrame, repository: Repository, output_srid: int = 4326
) -> list[dict]:
    """Query PostGIS for EDP boundary areas that intersect the uploaded geometry."""
    input_union = gdf.union_all()
    input_wkt = input_union.wkt
    input_area_sqm = input_union.area

    input_geom = ST_SetSRID(ST_GeomFromText(input_wkt), 27700)
    intersection = ST_CollectionExtract(
        ST_Intersection(EdpBoundaryLayer.geometry, input_geom), 3
    )

    stmt = select(
        EdpBoundaryLayer.name,
        EdpBoundaryLayer.attributes,
        ST_AsGeoJSON(ST_Transform(EdpBoundaryLayer.geometry, output_srid)).label(
            "edp_geojson"
        ),
        ST_AsGeoJSON(ST_Transform(intersection, output_srid)).label(
            "intersection_geojson"
        ),
        ST_Area(intersection).label("intersection_area_sqm"),
    ).where(
        ST_Intersects(
            EdpBoundaryLayer.geometry,
            input_geom,
        )
    )

    with repository.session() as session:
        rows = session.execute(stmt).fetchall()

    results = []
    for row in rows:
        area_sqm = row.intersection_area_sqm or 0.0
        edp_name = (row.attributes or {}).get("EDP_Name")
        results.append(
            {
                "label": edp_name,
                "n2k_site_name": edp_name,
                "edp_geometry": json.loads(row.edp_geojson),
                "intersection_geometry": json.loads(row.intersection_geojson),
                "overlap_area_ha": round(area_sqm / 10000.0, 4),
                "overlap_area_sqm": round(area_sqm, 2),
                "overlap_percentage": round((area_sqm / input_area_sqm) * 100, 2)
                if input_area_sqm > 0
                else 0.0,
            }
        )
    return results


@router.post(
    "/check-boundary",
    responses={
        400: {"description": "Invalid or unreadable geometry file"},
        413: {"description": "File too large"},
        422: {"description": "Boundary file has no CRS defined"},
    },
)
async def check_boundary(
    geometry_file: UploadFile,
    boundary_filename: Annotated[str | None, Form()] = None,
):
    """Check whether an uploaded geometry intersects with EDP areas.

    Supported formats:
    - .zip containing .shp (with companion .dbf, .shx, .prj files), .geojson, or .kml
    - .geojson or .json
    - .kml

    For zip uploads the caller (the backend service) passes `boundary_filename`:
    the bare filename of the entry that was selected during the backend's
    zip-safety validation step — today always a .shp, but the contract is
    format-agnostic. This service then opens that specific file rather than
    re-implementing a picking rule of its own.

    Returns the uploaded geometry as GeoJSON along with any intersecting EDP areas.
    """
    content = await geometry_file.read(_max_upload_bytes + 1)
    if len(content) > _max_upload_bytes:
        return _make_response(413, error="file_size_too_large")

    filename = geometry_file.filename or "input.geojson"
    ext = Path(filename).suffix.lower()

    try:
        _check_declared_geojson_crs(content, ext)
    except UnsupportedCRSError:
        return _make_response(422, error="unsupported_crs")

    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            gdf = _read_geometry(content, filename, Path(tmpdir), boundary_filename)
        except ValueError as e:
            return _make_response(400, error=str(e))

        # GeoJSON (RFC 7946) and KML (OGC spec) mandate WGS84 —
        # safe to assume EPSG:4326 when no CRS is present.
        if gdf.crs is None and ext in _WGS84_EXTENSIONS:
            gdf = gdf.set_crs(_WGS84)

        try:
            gdf = ensure_crs(gdf)
        except UnsupportedCRSError:
            return _make_response(422, error="unsupported_crs")
        except ValueError:
            return _make_response(422, error="missing_crs")

        validation_error = validate_geometry(gdf)

        if validation_error:
            # Keep the projected geometry to compute metadata bounds/centre so
            # the frontend can still zoom the map to the (invalid) boundary.
            geom_projected = gdf.geometry.iloc[0]
            gdf = gdf.to_crs(_WGS84)
            gdf = gdf.drop(columns=gdf.columns.difference(["geometry"]))
            geojson = json.loads(gdf.to_json())

            # Some rejected geometries (e.g. empty/corrupt) have no computable
            # bounds; skip metadata in that case rather than fail the response.
            boundary_metadata = None
            geom_wgs84 = gdf.geometry.iloc[0]
            if geom_projected is not None and not geom_projected.is_empty:
                boundary_metadata = _compute_boundary_metadata(
                    geom_projected, geom_wgs84
                )

            return _make_response(
                400,
                error=validation_error,
                boundary_geometry_wgs84=geojson,
                boundary_metadata=boundary_metadata,
            )

        repository = _get_repository()
        intersecting_edps = _find_intersecting_edps(gdf, repository, output_srid=4326)

        # Extract the first Polygon/MultiPolygon geometry, stripping user-supplied
        # properties to avoid processing Personal Identifiable Information (PII).
        polygons = gdf[gdf.geometry.geom_type.isin(_VALID_GEOM_TYPES)]
        if polygons.empty:
            return _make_response(400, error="no_polygon_found")
        first_geom = polygons.geometry.iloc[0]
        authority, code = gdf.crs.to_authority()
        crs_urn = f"urn:ogc:def:crs:{authority}::{code}"
        geom = first_geom.__geo_interface__
        boundary_geometry_original = {
            "type": geom["type"],
            "coordinates": geom["coordinates"],
            "crs": {
                "type": "name",
                "properties": {"name": crs_urn},
            },
        }

        polygons = polygons.to_crs(_WGS84)
        first_geom_wgs84 = polygons.geometry.iloc[0]
        boundary_geometry_wgs84 = first_geom_wgs84.__geo_interface__

        boundary_metadata = _compute_boundary_metadata(first_geom, first_geom_wgs84)

    return _make_response(
        boundary_geometry_original=boundary_geometry_original,
        boundary_geometry_wgs84=boundary_geometry_wgs84,
        intersecting_edps=intersecting_edps,
        boundary_metadata=boundary_metadata,
    )
