"""Boundary checking endpoint.

Accepts a geometry file (zip containing .shp/.prj, .geojson, or .kml)
and returns the extracted geometry as GeoJSON.
"""

import json
import logging
import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd
from fastapi import APIRouter, HTTPException, UploadFile
from fastapi.responses import JSONResponse

from app.config import ApiServerConfig

logger = logging.getLogger(__name__)

router = APIRouter()

_config = ApiServerConfig()
_max_upload_bytes = _config.max_upload_bytes

_SUPPORTED_EXTENSIONS = {".zip", ".geojson", ".json", ".kml", ".shp"}


def _read_geometry(content: bytes, filename: str, tmpdir: Path) -> gpd.GeoDataFrame:
    """Read a geometry file from uploaded bytes into a GeoDataFrame.

    Supports .geojson, .json, .shp, .kml, and .zip (containing .shp or .geojson).

    Args:
        content: Raw file bytes.
        filename: Original filename (used for extension detection).
        tmpdir: Temporary directory to write files into.

    Returns:
        GeoDataFrame with the uploaded geometries.

    Raises:
        HTTPException: If the file format is unsupported or unreadable.
    """
    suffix = Path(filename).suffix.lower()
    saved_path = tmpdir / filename
    saved_path.write_bytes(content)

    if suffix == ".zip":
        read_path = _extract_zip(saved_path, tmpdir)
    elif suffix in (".geojson", ".json", ".shp", ".kml"):
        read_path = saved_path
    else:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unsupported file format: {suffix}. "
                "Use .shp, .zip, .geojson, .json, or .kml"
            ),
        )

    try:
        if suffix == ".kml":
            return gpd.read_file(read_path, driver="KML")
        return gpd.read_file(read_path)
    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Failed to read geometry file: {e}"
        ) from e


def _extract_zip(zip_path: Path, tmpdir: Path) -> Path:
    """Extract a zip archive and return the path to the geometry file inside."""
    extract_dir = tmpdir / "extracted"
    extract_dir.mkdir()
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)

    shp_files = list(extract_dir.glob("**/*.shp"))
    geojson_files = list(extract_dir.glob("**/*.geojson"))
    kml_files = list(extract_dir.glob("**/*.kml"))

    if shp_files:
        return shp_files[0]
    if geojson_files:
        return geojson_files[0]
    if kml_files:
        return kml_files[0]

    raise HTTPException(
        status_code=400,
        detail="Zip file must contain a .shp, .geojson, or .kml file",
    )


@router.post(
    "/check-boundary",
    responses={
        400: {"description": "Invalid or unreadable geometry file"},
        413: {"description": "File too large"},
    },
)
async def check_boundary(geometry_file: UploadFile):
    """Accept a geometry file and return the extracted geometry as GeoJSON.

    Supported formats:
    - .zip containing .shp and .prj files
    - .geojson or .json
    - .kml
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

        geojson = json.loads(gdf.to_json())

    return JSONResponse(content=geojson)
