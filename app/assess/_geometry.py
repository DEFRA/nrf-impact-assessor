"""Shared geometry reading helpers for API endpoints.

Extracts common logic for reading uploaded geometry files (GeoJSON, Shapefile, zip)
and injecting job metadata fields into GeoDataFrames.
"""

import zipfile
from pathlib import Path

import geopandas as gpd
from fastapi import HTTPException


def read_geometry_from_upload(
    content: bytes, filename: str, tmpdir: Path
) -> gpd.GeoDataFrame:
    """Read a geometry file from uploaded bytes into a GeoDataFrame.

    Supports .geojson, .json, .shp, and .zip (containing .shp or .geojson).

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
        extract_dir = tmpdir / "extracted"
        extract_dir.mkdir()
        with zipfile.ZipFile(saved_path, "r") as zf:
            zf.extractall(extract_dir)
        shp_files = list(extract_dir.glob("**/*.shp"))
        geojson_files = list(extract_dir.glob("**/*.geojson"))
        if shp_files:
            read_path = shp_files[0]
        elif geojson_files:
            read_path = geojson_files[0]
        else:
            raise HTTPException(
                status_code=400,
                detail="Zip file must contain a .shp or .geojson file",
            )
    elif suffix in (".geojson", ".json", ".shp"):
        read_path = saved_path
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file format: {suffix}. Use .shp, .zip, .geojson, or .json",
        )

    try:
        return gpd.read_file(read_path)
    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Failed to read geometry file: {e}"
        ) from e


def inject_job_fields(
    gdf: gpd.GeoDataFrame,
    job_id: str,
    name: str,
    dwelling_type: str,
    dwellings: int,
) -> gpd.GeoDataFrame:
    """Inject standard job metadata columns into a GeoDataFrame.

    Args:
        gdf: Input GeoDataFrame.
        job_id: Unique job identifier.
        name: Development name.
        dwelling_type: Dwelling category (e.g. "house").
        dwellings: Number of dwellings.

    Returns:
        The same GeoDataFrame with added columns.
    """
    gdf["id"] = job_id
    gdf["name"] = name
    gdf["dwelling_category"] = dwelling_type
    gdf["source"] = "api"
    gdf["dwellings"] = dwellings
    gdf["area_m2"] = gdf.geometry.area
    return gdf
