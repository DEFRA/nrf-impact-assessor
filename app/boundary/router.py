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
    ST_CollectionExtract,
    ST_GeomFromText,
    ST_Intersection,
    ST_Intersects,
    ST_Relate,
    ST_SetSRID,
    ST_Union,
)
from pyproj import CRS
from pyproj.exceptions import CRSError
from sqlalchemy import select

from app.boundary.validation import (
    SUPPORTED_CRS,
    validate_coordinate_range,
    validate_geometry,
)
from app.config import ApiServerConfig
from app.data_sync.active_version import get_active_version
from app.models.db import EdpBoundaryLayer, EdpExcludedAreas, NnCatchments
from app.repositories.engine import get_shared_repository
from app.repositories.repository import Repository
from app.spatial.utils import UnsupportedCRSError, ensure_crs

logger = logging.getLogger(__name__)

_VALID_GEOM_TYPES = {"Polygon"}
_WGS84 = "EPSG:4326"

# Stands in for an exclusion zone whose name is missing or blank. The boundary
# is still ineligible, so the overlap must be reported even when we cannot say
# which site caused it.
_UNNAMED_EXCLUDED_AREA = "Unnamed exclusion area"

# Smallest overlap with an exclusion zone that makes a boundary ineligible.
#
# An exact touch between a drawn boundary and a zone edge must not exclude, but
# floating-point intersection rarely lands exactly: a shared edge can come back
# with a sliver of area measured in square millimetres. This floor absorbs that
# arithmetic noise so a boundary drawn deliberately along a zone edge stays
# eligible.
#
# It is a noise floor, not a drawing-error allowance. At 0.001 m² (10 cm², a
# 1 mm depth across 1 m of zone edge) any real contact clears it — including
# the hand-drawn slips seen in reports, which run 0.005-0.008 m² at 8-12 cm
# deep and are therefore excluded. Raising it towards 0.1 m² would forgive
# those too; that is a policy decision, not a numerical one.
_MIN_EXCLUSION_OVERLAP_SQM = 0.001


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
    intersecting_excluded_areas: list | None = None,
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
            "intersectingExcludedAreas": intersecting_excluded_areas or [],
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
    except json.JSONDecodeError, UnicodeDecodeError, KeyError, TypeError:
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


def _iter_polygon_geometries(node):
    """Yield each Polygon geometry dict found in a GeoJSON Geometry,
    Feature, or FeatureCollection."""
    if not isinstance(node, dict):
        return

    node_type = node.get("type")
    if node_type == "FeatureCollection":
        for feature in node.get("features") or []:
            yield from _iter_polygon_geometries(feature)
    elif node_type == "Feature":
        yield from _iter_polygon_geometries(node.get("geometry"))
    elif node_type == "GeometryCollection":
        for geometry in node.get("geometries") or []:
            yield from _iter_polygon_geometries(geometry)
    elif node_type == "Polygon":
        yield node


def _close_unclosed_rings(content: bytes, ext: str) -> tuple[bytes, bool]:
    """Close any unclosed ring (exterior or interior/hole) in every Polygon
    geometry in a GeoJSON/JSON file's raw bytes.

    GDAL silently closes unclosed rings on read (RFC 7946 requires closed
    rings) rather than surfacing an error — even with
    OGR_GEOMETRY_ACCEPT_UNCLOSED_RING=NO it just drops the ring, producing a
    valid-but-empty geometry with no way to tell what went wrong. We close
    rings ourselves first so GDAL/validate_geometry can parse and check the
    shape normally.

    Only the *exterior* ring's closure state is reported back. An unclosed
    interior ring (hole) is still closed here so GDAL can parse the
    geometry, but is left to be reported via the normal geometry_has_holes
    validation error rather than unclosed_ring — the single-ring preview
    built for unclosed_ring can't represent "a hole was unclosed" without
    fabricating a break in an exterior ring that was never actually broken.

    Returns:
        A (content, exterior_was_unclosed) tuple. `content` reflects any
        ring closures (exterior or interior); `exterior_was_unclosed`
        reflects only the exterior ring.
    """
    if ext not in _GEOJSON_EXTENSIONS:
        return content, False

    try:
        data = json.loads(content)
    except json.JSONDecodeError, UnicodeDecodeError:
        return content, False

    any_ring_closed = False
    exterior_was_unclosed = False
    for polygon in _iter_polygon_geometries(data):
        for index, ring in enumerate(polygon.get("coordinates") or []):
            if isinstance(ring, list) and len(ring) >= 2 and ring[0] != ring[-1]:
                ring.append(ring[0])
                any_ring_closed = True
                if index == 0:
                    exterior_was_unclosed = True

    if not any_ring_closed:
        return content, False

    return json.dumps(data).encode("utf-8"), exterior_was_unclosed


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


def _find_intersecting_excluded_areas(
    gdf: gpd.GeoDataFrame, repository: Repository
) -> list[str]:
    """Return names of EDP exclusion zones the uploaded geometry overlaps.

    A boundary overlapping any exclusion zone is not eligible for the EDP and
    must be routed to HRA instead, so the caller treats a non-empty result as
    ineligibility.

    The overlap must exceed `_MIN_EXCLUSION_OVERLAP_SQM` to count. The exclusion
    zones are already buffered SSSI polygons, so a boundary that touches a zone
    edge lies outside the zone; the floor only absorbs the square-millimetre
    sliver an exact touch can leave behind. Any real entry, down to the
    centimetre-deep slips seen when drawing by hand, exceeds it and excludes.

    Names are deduplicated (one SSSI may be several polygons) and sorted so the
    response is deterministic.

    A row whose name is NULL or blank becomes `_UNNAMED_EXCLUDED_AREA` rather
    than being dropped. Dropping it would be a fail-*open* bug: the caller gates
    on this list being non-empty, so a nameless zone would read as "no overlap"
    and the boundary would wrongly continue down the EDP route. Detection has to
    depend only on geometry, never on attribute quality — the QC rules reduce
    how often the placeholder is needed, they do not make it unnecessary.
    """
    input_geom = ST_SetSRID(ST_GeomFromText(gdf.union_all().wkt), 27700)
    overlap_area = ST_Area(
        ST_CollectionExtract(ST_Intersection(EdpExcludedAreas.geometry, input_geom), 3)
    )

    with repository.session() as session:
        version = get_active_version(session, "edp_excluded_areas")
        stmt = (
            select(EdpExcludedAreas.name)
            .where(
                EdpExcludedAreas.version == version,
                # Three filters, cheapest first. ST_Intersects is index-backed
                # and narrows candidates; ST_Relate '2********' (interiors share
                # area) drops touch-only contact without building a geometry;
                # only what survives both pays for the intersection the area
                # threshold needs — on the 31k-vertex Wensum polygon that is
                # ~93ms against ~12ms, so it must stay off the common path.
                ST_Intersects(EdpExcludedAreas.geometry, input_geom),
                ST_Relate(EdpExcludedAreas.geometry, input_geom, "2********"),
                overlap_area > _MIN_EXCLUSION_OVERLAP_SQM,
            )
            .distinct()
        )
        rows = session.execute(stmt).fetchall()

    return sorted(
        {
            row.name.strip()
            if row.name and row.name.strip()
            else _UNNAMED_EXCLUDED_AREA
            for row in rows
        }
    )


def _find_intersecting_edps(
    gdf: gpd.GeoDataFrame, repository: Repository
) -> list[dict]:
    """Query PostGIS for EDP boundary areas that intersect the uploaded geometry.

    Only the name and overlap measures are returned. The EDP and intersection
    polygons are deliberately left out: no consumer draws them, and serialising
    them (an EDP boundary runs to tens of thousands of vertices) dominated both
    the query cost and the size of the JSON carried through to the quote job.
    """
    input_union = gdf.union_all()
    input_wkt = input_union.wkt
    input_area_sqm = input_union.area

    input_geom = ST_SetSRID(ST_GeomFromText(input_wkt), 27700)
    intersection = ST_CollectionExtract(
        ST_Intersection(EdpBoundaryLayer.geometry, input_geom), 3
    )

    with repository.session() as session:
        # Without this the query spans every loaded version, so once a data sync
        # has staged v2 alongside v1 each EDP would be reported twice.
        version = get_active_version(session, "edp_boundary_layer")
        stmt = select(
            EdpBoundaryLayer.name,
            EdpBoundaryLayer.attributes,
            ST_Area(intersection).label("intersection_area_sqm"),
        ).where(
            EdpBoundaryLayer.version == version,
            ST_Intersects(
                EdpBoundaryLayer.geometry,
                input_geom,
            ),
        )
        rows = session.execute(stmt).fetchall()

    results = []
    for row in rows:
        area_sqm = row.intersection_area_sqm or 0.0
        edp_name = (row.attributes or {}).get("EDP_Name")
        results.append(
            {
                "label": edp_name,
                "overlap_area_ha": round(area_sqm / 10000.0, 4),
                "overlap_area_sqm": round(area_sqm, 2),
                "overlap_percentage": round((area_sqm / input_area_sqm) * 100, 2)
                if input_area_sqm > 0
                else 0.0,
            }
        )
    return results


def _find_intersecting_catchments(
    gdf: gpd.GeoDataFrame, repository: Repository
) -> list[dict]:
    """Query PostGIS for the NN catchments the uploaded boundary falls in.

    `catchmentOverlapPercentage` is the share of the *boundary* in each
    catchment, same denominator as the sibling `overlap_percentage`.

    One catchment is several polygons, so grouping happens in SQL: dissolving
    per name before dividing stops it being reported once per polygon.

    The dissolve is ST_Union, not SUM. Same-name polygons are not guaranteed
    disjoint — the loaded data has Broads features overlapping by ~257 m2 — and
    summing their intersections counts the shared strip once per polygon. A
    boundary lying inside such an overlap would report 200%.
    """
    input_union = gdf.union_all()
    input_area_sqm = input_union.area

    input_geom = ST_SetSRID(ST_GeomFromText(input_union.wkt), 27700)
    intersection = ST_CollectionExtract(
        ST_Intersection(NnCatchments.geometry, input_geom), 3
    )
    label = NnCatchments.attributes["N2K_Site_N"].astext

    with repository.session() as session:
        version = get_active_version(session, "nn_catchments")
        overlap_area = ST_Area(ST_Union(intersection))
        stmt = (
            select(
                label.label("label"),
                overlap_area.label("overlap_area_sqm"),
            )
            .where(
                NnCatchments.version == version,
                ST_Intersects(NnCatchments.geometry, input_geom),
            )
            .group_by(label)
            # ST_Intersects is true for an edge-only touch, which has no area.
            .having(overlap_area > 0)
        )
        rows = session.execute(stmt).fetchall()

    results = []
    for row in rows:
        if not row.label or not row.label.strip():
            continue
        area_sqm = row.overlap_area_sqm or 0.0
        results.append(
            {
                "label": row.label.strip(),
                "catchmentOverlapPercentage": round(
                    (area_sqm / input_area_sqm) * 100, 2
                )
                if input_area_sqm > 0
                else 0.0,
            }
        )
    return sorted(results, key=lambda c: c["label"])


def _attach_catchments(
    intersecting_edps: list[dict], gdf: gpd.GeoDataFrame, repository: Repository
) -> None:
    """Add the boundary's NN catchments to each EDP entry, in place.

    Kept separate from _find_intersecting_edps: the catchments are a property
    of the boundary, not of any one EDP, so the same list rides on every entry
    and neither query needs to know about the other.

    With no EDP there is nothing to attach to, so the query is skipped.
    """
    if not intersecting_edps:
        return
    catchments = _find_intersecting_catchments(gdf, repository)
    for edp in intersecting_edps:
        edp["catchments"] = catchments


def _build_invalid_geometry_response(
    gdf: gpd.GeoDataFrame, error_code: str, *, reopen_exterior_ring: bool
) -> JSONResponse:
    """Build the 400 response for an invalid/rejected geometry, still
    including the (invalid) boundary so the frontend can preview it.

    Args:
        gdf: Single-row GeoDataFrame holding the geometry to preview, in
            its post-validation CRS (not yet reprojected to WGS84).
        error_code: The failure code to report.
        reopen_exterior_ring: If True, remove the final (duplicate) point
            from the WGS84 exterior ring — used when the ring was only
            closed internally so GDAL could parse it, so the map shows the
            boundary exactly as unclosed as it was uploaded.
    """
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
        boundary_metadata = _compute_boundary_metadata(geom_projected, geom_wgs84)

    if reopen_exterior_ring:
        exterior = geojson["features"][0]["geometry"]["coordinates"][0]
        if len(exterior) > 1 and exterior[0] == exterior[-1]:
            exterior.pop()

    return _make_response(
        400,
        error=error_code,
        boundary_geometry_wgs84=geojson,
        boundary_metadata=boundary_metadata,
    )


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

    Returns the uploaded geometry as GeoJSON along with any intersecting EDP
    areas and any intersecting EDP exclusion zones.

    A boundary that overlaps an exclusion zone (a buffered SSSI polygon) is not
    eligible for the EDP and must be routed to HRA. Such a response is a normal
    200 with `error: null`, and a non-empty `intersectingExcludedAreas` is the
    *sole* signal of that. Sharing a zone edge exactly does not exclude;
    anything past it does, down to a centimetre.

    `intersectingEdps` is NOT that signal and must not be read as one. The
    zones are clipped SSSI extents lying inside the EDP boundary, so an
    excluded boundary overlaps the EDP too and that overlap is reported like
    any other. Gating eligibility on a non-empty `intersectingEdps` would route
    an excluded boundary down the EDP path instead of to HRA.
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

    content, exterior_was_unclosed = _close_unclosed_rings(content, ext)

    with tempfile.TemporaryDirectory() as tmpdir:
        gdf, error_response = _load_and_validate_geometry(
            content,
            filename,
            ext,
            Path(tmpdir),
            boundary_filename,
            exterior_was_unclosed,
        )
        if error_response is not None:
            return error_response

        return _assess_boundary(gdf)


def _load_and_validate_geometry(
    content: bytes,
    filename: str,
    ext: str,
    tmpdir: Path,
    boundary_filename: str | None,
    exterior_was_unclosed: bool,
) -> tuple[gpd.GeoDataFrame | None, JSONResponse | None]:
    """Read an uploaded geometry file, resolve its CRS, and validate it.

    Returns:
        (gdf, None) on success, or (None, error_response) on any failure.
    """
    try:
        gdf = _read_geometry(content, filename, tmpdir, boundary_filename)
    except ValueError as e:
        return None, _make_response(400, error=str(e))

    # GeoJSON (RFC 7946) and KML (OGC spec) mandate WGS84 —
    # safe to assume EPSG:4326 when no CRS is present.
    if gdf.crs is None and ext in _WGS84_EXTENSIONS:
        gdf = gdf.set_crs(_WGS84)

    # Must run before ensure_crs()/reprojection: out-of-domain coordinates
    # pass geometry validation unnoticed and only surface later as a
    # reprojection crash (see validate_coordinate_range).
    coordinate_range_error = validate_coordinate_range(gdf)
    if coordinate_range_error:
        return None, _make_response(400, error=coordinate_range_error)

    try:
        gdf = ensure_crs(gdf)
    except UnsupportedCRSError:
        return None, _make_response(422, error="unsupported_crs")
    except ValueError:
        return None, _make_response(422, error="missing_crs")

    validation_error = validate_geometry(gdf)

    # Only report unclosed_ring in place of another validation error when
    # the exterior ring itself needed closing — an unclosed hole is left
    # for geometry_has_holes to report instead (see _close_unclosed_rings).
    # A MultiPolygon (already rejected as unsupported_geometry_type) also
    # can't be safely re-opened using the single-ring logic below.
    report_unclosed_ring = (
        exterior_was_unclosed and gdf.geometry.iloc[0].geom_type == "Polygon"
    )

    if report_unclosed_ring or validation_error:
        error_code = "unclosed_ring" if report_unclosed_ring else validation_error
        return None, _build_invalid_geometry_response(
            gdf, error_code, reopen_exterior_ring=report_unclosed_ring
        )

    return gdf, None


def _assess_boundary(gdf: gpd.GeoDataFrame) -> JSONResponse:
    """Find EDP/exclusion-zone intersections and build the response for an
    already-validated geometry."""
    repository = _get_repository()
    intersecting_excluded_areas = _find_intersecting_excluded_areas(gdf, repository)
    # Reported even when the boundary is excluded. The zones are clipped SSSI
    # extents lying inside the EDP, so an excluded boundary really does overlap
    # it and an empty list here would be a geometric falsehood. Ineligibility
    # is carried by intersecting_excluded_areas alone.
    intersecting_edps = _find_intersecting_edps(gdf, repository)
    _attach_catchments(intersecting_edps, gdf, repository)

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
        intersecting_excluded_areas=intersecting_excluded_areas,
        boundary_metadata=boundary_metadata,
    )
