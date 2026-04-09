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
from sqlalchemy import select

from app.boundary.validation import SUPPORTED_CRS, validate_geometry
from app.config import ApiServerConfig, DatabaseSettings
from app.models.db import EdpBoundaryLayer
from app.repositories.engine import create_db_engine
from app.repositories.repository import Repository
from app.spatial.utils import UnsupportedCRSError, ensure_crs

logger = logging.getLogger(__name__)

_VALID_GEOM_TYPES = {"Polygon"}
_WGS84 = "EPSG:4326"


router = APIRouter()

_config = ApiServerConfig()
_max_upload_bytes = _config.max_upload_bytes

# ---------------------------------------------------------------------------
# Lazy-initialised repository singleton
# ---------------------------------------------------------------------------
_repository: Repository | None = None


def _get_repository() -> Repository:
    """Get or create the module-level Repository singleton."""
    global _repository
    if _repository is None:
        logger.info("Initialising Repository for /check-boundary endpoint...")
        db_settings = DatabaseSettings()
        engine = create_db_engine(db_settings, pool_size=2, max_overflow=2)
        _repository = Repository(engine)
        logger.info("Repository initialised")
    return _repository


_EXT_GEOJSON = ".geojson"
_EXT_JSON = ".json"
_EXT_KML = ".kml"
_EXT_ZIP = ".zip"
_GEOJSON_EXTENSIONS = frozenset({_EXT_GEOJSON, _EXT_JSON})
_WGS84_EXTENSIONS = frozenset({_EXT_GEOJSON, _EXT_JSON, _EXT_KML})
_SUPPORTED_EXTENSIONS = frozenset({_EXT_ZIP, _EXT_GEOJSON, _EXT_JSON, _EXT_KML})


def _make_response(
    status_code: int = 200,
    *,
    boundary_geometry_original: dict | None = None,
    boundary_geometry_wgs84: dict | None = None,
    intersecting_edps: list | None = None,
    error: str | None = None,
) -> JSONResponse:
    """Build a consistent JSON response for the check-boundary endpoint."""
    return JSONResponse(
        status_code=status_code,
        content={
            "boundaryGeometryOriginal": boundary_geometry_original,
            "boundaryGeometryWgs84": boundary_geometry_wgs84,
            "intersectingEdps": intersecting_edps or [],
            "error": error,
        },
    )


def _validate_extension(filename: str) -> str:
    """Validate the file extension and return it.

    Only used for control flow (comparisons), never in path construction.
    """
    suffix = Path(filename).suffix.lower()
    if suffix not in _SUPPORTED_EXTENSIONS:
        msg = f"Unsupported file format: {suffix}. Use .zip, .geojson, .json, or .kml"
        raise ValueError(msg)
    return suffix


def _write_to_temp(content: bytes, tmpdir: Path, suffix: str) -> Path:
    """Write content to a system-generated temporary file.

    Uses tempfile.NamedTemporaryFile so the path is entirely OS-generated
    with no user-controlled data in the filename.
    """
    with tempfile.NamedTemporaryFile(dir=tmpdir, suffix=suffix, delete=False) as tmp:
        tmp.write(content)
        return Path(tmp.name)


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
        msg = f"Failed to read geometry file: {e}"
        raise ValueError(msg) from e


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
                msg = "Malicious zip entry detected"
                raise ValueError(msg)
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

    msg = "Zip file must contain a .shp, .geojson, or .kml file"
    raise ValueError(msg)


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
        msg = f"Boundary file {boundary_filename!r} not found inside uploaded zip."
        raise ValueError(msg)
    # Multiple matches would mean the same filename appears in two different
    # subdirectories — ambiguous, so refuse rather than guess.
    if len(candidates) > 1:
        msg = (
            f"Boundary file {boundary_filename!r} appears more than once in the "
            "uploaded zip; cannot determine which one to use."
        )
        raise ValueError(msg)
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
        msg = (
            f"Shapefile is missing required companion files: "
            f"{', '.join(missing)}. "
            "A zip must contain .shp, .dbf, and .shx files."
        )
        raise ValueError(msg)
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
        results.append(
            {
                "label": (row.attributes or {}).get("Label"),
                "n2k_site_name": (row.attributes or {}).get("N2K_Site_N"),
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
        max_mb = _max_upload_bytes / (1024 * 1024)
        return _make_response(
            413,
            error=f"File too large. Maximum upload size is {max_mb:.0f} MB.",
        )

    filename = geometry_file.filename or "input.geojson"

    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            gdf = _read_geometry(content, filename, Path(tmpdir), boundary_filename)
        except ValueError as e:
            return _make_response(400, error=str(e))

        # GeoJSON (RFC 7946) and KML (OGC spec) mandate WGS84 —
        # safe to assume EPSG:4326 when no CRS is present.
        ext = Path(filename).suffix.lower()
        if gdf.crs is None and ext in _WGS84_EXTENSIONS:
            gdf = gdf.set_crs(_WGS84)

        try:
            gdf = ensure_crs(gdf)
        except UnsupportedCRSError:
            supported = ", ".join(
                f"EPSG:{code} ({label})" for code, label in SUPPORTED_CRS.items()
            )
            detail = (
                "The uploaded boundary file uses an unsupported"
                " coordinate reference system (CRS)."
                f" Supported coordinate systems are: {supported}."
                " Please ensure your boundary file uses one of these"
                " Coordinate Reference Systems and try again."
            )
            return _make_response(422, error=detail)
        except ValueError:
            supported = ", ".join(
                f"EPSG:{code} ({label})" for code, label in SUPPORTED_CRS.items()
            )
            detail = (
                "The uploaded boundary file has no coordinate reference system (CRS) "
                "defined."
            )
            if ext == _EXT_ZIP:
                detail += " Shapefiles require a .prj file to specify the CRS."
            detail += (
                f" Supported coordinate systems are: {supported}."
                " Please ensure your boundary file has one of these"
                " Coordinate Reference Systems defined and try again."
            )
            return _make_response(422, error=detail)

        validation_error = validate_geometry(gdf)

        if validation_error:
            gdf = gdf.to_crs(_WGS84)
            gdf = gdf.drop(columns=gdf.columns.difference(["geometry"]))
            geojson = json.loads(gdf.to_json())

            return _make_response(
                400,
                error=validation_error,
                boundary_geometry_wgs84=geojson,
            )

        repository = _get_repository()
        intersecting_edps = _find_intersecting_edps(gdf, repository, output_srid=4326)

        # Extract the first Polygon/MultiPolygon geometry, stripping user-supplied
        # properties to avoid processing Personal Identifiable Information (PII).
        polygons = gdf[gdf.geometry.geom_type.isin(_VALID_GEOM_TYPES)]
        if polygons.empty:
            return _make_response(
                400,
                error="No polygon geometry found in the uploaded file",
            )
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

    return _make_response(
        boundary_geometry_original=boundary_geometry_original,
        boundary_geometry_wgs84=boundary_geometry_wgs84,
        intersecting_edps=intersecting_edps,
    )
