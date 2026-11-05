"""Reading and validating boundaries, shared by the boundary endpoints.

`load_boundary` turns raw bytes into a validated GeoDataFrame in British National
Grid, or raises `BoundaryRejectedError`. It knows nothing of HTTP responses: each
endpoint reports a rejection in its own format.
"""

import json
import tempfile
import zipfile
from io import BytesIO
from pathlib import Path

import geopandas as gpd
from pyproj import CRS
from pyproj.exceptions import CRSError

from app.boundary.validation import (
    SUPPORTED_CRS,
    validate_coordinate_range,
    validate_geometry,
)
from app.spatial.utils import UnsupportedCRSError, ensure_crs

WGS84 = "EPSG:4326"

_EXT_GEOJSON = ".geojson"
_EXT_JSON = ".json"
_EXT_KML = ".kml"
_EXT_ZIP = ".zip"
_GEOJSON_EXTENSIONS = frozenset({_EXT_GEOJSON, _EXT_JSON})
_SUPPORTED_EXTENSIONS = frozenset({_EXT_ZIP, _EXT_GEOJSON, _EXT_JSON, _EXT_KML})


class BoundaryRejectedError(Exception):
    """A boundary that cannot be used, for each endpoint to report in its own way.

    Attributes:
        status_code: HTTP status for the rejection.
        code: Failure code, shared across the boundary endpoints.
        rejected_gdf: The geometry, when it was read far enough to be previewed.
        exterior_was_unclosed: The exterior ring was closed only so GDAL could
            parse it, so a preview must show it open again.
    """

    def __init__(
        self,
        status_code: int,
        code: str,
        *,
        rejected_gdf: gpd.GeoDataFrame | None = None,
        exterior_was_unclosed: bool = False,
    ) -> None:
        super().__init__(code)
        self.status_code = status_code
        self.code = code
        self.rejected_gdf = rejected_gdf
        self.exterior_was_unclosed = exterior_was_unclosed


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


def load_boundary(
    content: bytes, filename: str, boundary_filename: str | None = None
) -> gpd.GeoDataFrame:
    """Read a boundary file, resolve its CRS, and validate it.

    Synchronous and slow enough to matter, as parsing goes through GDAL and may
    write to disk: async callers must run it in a worker thread.

    Args:
        content: Raw file bytes.
        filename: Original filename, whose extension selects the format.
        boundary_filename: Bare filename of the entry to read from a zip. See
            `_read_geometry`.

    Returns:
        The boundary in British National Grid.

    Raises:
        BoundaryRejectedError: If the boundary cannot be used.
    """
    ext = Path(filename).suffix.lower()

    # These read the raw JSON before GDAL has vetted it, so anything can turn up:
    # a member of the wrong type, a number too long to parse, or nesting too deep
    # to walk.
    try:
        _check_declared_geojson_crs(content, ext)
        content, exterior_was_unclosed = _close_unclosed_rings(content, ext)
    except UnsupportedCRSError:
        raise BoundaryRejectedError(422, "unsupported_crs") from None
    except TypeError, ValueError, RecursionError:
        raise BoundaryRejectedError(400, "unreadable_geometry_file") from None

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            gdf = _read_geometry(content, filename, Path(tmpdir), boundary_filename)
    except ValueError as e:
        raise BoundaryRejectedError(400, str(e)) from None

    # Must run before ensure_crs()/reprojection: out-of-domain coordinates
    # pass geometry validation unnoticed and only surface later as a
    # reprojection crash (see validate_coordinate_range).
    coordinate_range_error = validate_coordinate_range(gdf)
    if coordinate_range_error:
        raise BoundaryRejectedError(400, coordinate_range_error)

    try:
        gdf = ensure_crs(gdf)
    except UnsupportedCRSError:
        raise BoundaryRejectedError(422, "unsupported_crs") from None
    except ValueError:
        raise BoundaryRejectedError(422, "missing_crs") from None

    if gdf.empty:
        raise BoundaryRejectedError(400, "no_polygon_found")

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
        raise BoundaryRejectedError(
            400,
            error_code,
            rejected_gdf=gdf,
            exterior_was_unclosed=report_unclosed_ring,
        )

    return gdf
