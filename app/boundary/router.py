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
from fastapi import APIRouter, HTTPException, Query, UploadFile
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

from app.config import ApiServerConfig, DatabaseSettings
from app.models.db import EdpBoundaryLayer
from app.repositories.engine import create_db_engine
from app.repositories.repository import Repository
from app.spatial.utils import ensure_crs

logger = logging.getLogger(__name__)

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


def _validate_extension(filename: str) -> str:
    """Validate the file extension and return it.

    Only used for control flow (comparisons), never in path construction.
    """
    suffix = Path(filename).suffix.lower()
    if suffix not in _SUPPORTED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unsupported file format: {suffix}. Use .zip, .geojson, .json, or .kml"
            ),
        )
    return suffix


def _write_to_temp(content: bytes, tmpdir: Path, suffix: str) -> Path:
    """Write content to a system-generated temporary file.

    Uses tempfile.NamedTemporaryFile so the path is entirely OS-generated
    with no user-controlled data in the filename.
    """
    with tempfile.NamedTemporaryFile(dir=tmpdir, suffix=suffix, delete=False) as tmp:
        tmp.write(content)
        return Path(tmp.name)


def _read_geometry(content: bytes, filename: str, tmpdir: Path) -> gpd.GeoDataFrame:
    """Read a geometry file from uploaded bytes into a GeoDataFrame.

    Supports .geojson, .json, .kml, and .zip (containing .shp, .geojson, or .kml).

    Args:
        content: Raw file bytes.
        filename: Original filename (used for extension detection).
        tmpdir: Temporary directory to write files into.

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
            read_path = _extract_zip(zip_path, tmpdir)
            return gpd.read_file(read_path)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Failed to read geometry file: {e}"
        ) from e


def _extract_zip(zip_path: Path, tmpdir: Path) -> Path:
    """Extract a zip archive and return the path to the geometry file inside."""
    extract_dir = tmpdir / "extracted"
    extract_dir.mkdir()
    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.infolist():
            member_path = (extract_dir / member.filename).resolve()
            if not member_path.is_relative_to(extract_dir.resolve()):
                raise HTTPException(
                    status_code=400, detail="Malicious zip entry detected"
                )
        zf.extractall(extract_dir)

    shp_files = list(extract_dir.glob("**/*.shp"))
    geojson_files = list(extract_dir.glob("**/*.geojson"))
    kml_files = list(extract_dir.glob("**/*.kml"))

    if shp_files:
        shp_path = shp_files[0]
        stem = shp_path.stem
        shp_dir = shp_path.parent
        missing = [
            ext for ext in (".dbf", ".shx") if not (shp_dir / f"{stem}{ext}").exists()
        ]
        if missing:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Shapefile is missing required companion files: "
                    f"{', '.join(missing)}. "
                    "A zip must contain .shp, .dbf, and .shx files."
                ),
            )
        return shp_path
    if geojson_files:
        return geojson_files[0]
    if kml_files:
        return kml_files[0]

    raise HTTPException(
        status_code=400,
        detail="Zip file must contain a .shp, .geojson, or .kml file",
    )


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
    proj: Annotated[
        str, Query(description="Output projection (e.g. 'EPSG:4326')")
    ] = "EPSG:4326",
):
    """Check whether an uploaded geometry intersects with EDP areas.

    Supported formats:
    - .zip containing .shp (with companion .dbf, .shx, .prj files), .geojson, or .kml
    - .geojson or .json
    - .kml

    Returns the uploaded geometry as GeoJSON along with any intersecting EDP areas.
    """
    content = await geometry_file.read(_max_upload_bytes + 1)
    if len(content) > _max_upload_bytes:
        max_mb = _max_upload_bytes / (1024 * 1024)
        raise HTTPException(
            status_code=413,
            detail=f"File too large. Maximum upload size is {max_mb:.0f} MB.",
        )

    filename = geometry_file.filename or "input.geojson"

    with tempfile.TemporaryDirectory() as tmpdir:
        gdf = _read_geometry(content, filename, Path(tmpdir))

        # GeoJSON (RFC 7946) and KML (OGC spec) mandate WGS84 —
        # safe to assume EPSG:4326 when no CRS is present.
        ext = Path(filename).suffix.lower()
        if gdf.crs is None and ext in _WGS84_EXTENSIONS:
            gdf = gdf.set_crs("EPSG:4326")

        try:
            gdf = ensure_crs(gdf)
        except ValueError:
            detail = (
                "The uploaded boundary file has no coordinate reference system (CRS) "
                "defined."
            )
            if ext == _EXT_ZIP:
                detail += " Shapefiles require a .prj file to specify the CRS."
            detail += (
                " Please ensure your boundary file has the appropriate"
                " Coordinate Reference System defined."
            )
            raise HTTPException(status_code=422, detail=detail) from None

        repository = _get_repository()
        output_srid = int(proj.split(":")[1])
        intersecting_edps = _find_intersecting_edps(gdf, repository, output_srid)

        gdf = gdf.to_crs(proj)
        gdf = gdf.drop(columns=gdf.columns.difference(["geometry"]))

        geojson = json.loads(gdf.to_json())

    return JSONResponse(
        content={
            "geometry": geojson,
            "intersecting_edps": intersecting_edps,
            "intersects_edp": len(intersecting_edps) > 0,
        }
    )
