#!/usr/bin/env python

"""Extract test fixture data by clipping reference layers to test input extents.

Run once locally against the full production dataset. Output is committed to
tests/data/fixtures/ and loaded into PostGIS at CI test time.

Usage:
    cd scripts
    uv run python extract_test_fixtures.py
    uv run python extract_test_fixtures.py --buffer 2000
    uv run python extract_test_fixtures.py --output-dir /tmp/fixtures
"""

import sqlite3
from pathlib import Path

import geopandas as gpd
import pandas as pd
import shapely
import typer
from settings import ScriptSettings
from shapely.ops import unary_union

_PROJECT_ROOT = Path(__file__).parent.parent
_FIXTURES_DIR = _PROJECT_ROOT / "tests" / "data" / "fixtures"
_TEST_INPUTS_DIR = _PROJECT_ROOT / "tests" / "data" / "inputs"
_CRS_BNG = "EPSG:27700"

app = typer.Typer(help="Extract test fixture data from reference layers")


def _collect_test_input_geometries(inputs_dir: Path) -> list[shapely.Geometry]:
    """Collect all geometries from test input files (GeoJSON and shapefiles)."""
    geometries = []

    for path in list(inputs_dir.rglob("*.geojson")) + list(inputs_dir.rglob("*.shp")):
        try:
            gdf = gpd.read_file(path)
            if gdf.crs is None:
                gdf = gdf.set_crs(_CRS_BNG)
            elif gdf.crs.to_epsg() != 27700:
                gdf = gdf.to_crs(_CRS_BNG)
            valid = gdf.geometry.dropna()
            geometries.extend(valid.tolist())
            print(f"  {path.relative_to(_PROJECT_ROOT)}: {len(valid)} features")
        except Exception as e:
            print(f"  Warning: could not read {path.name}: {e}")

    return geometries


def _compute_clip_extent(
    geometries: list[shapely.Geometry], buffer_m: float
) -> shapely.Geometry:
    """Return union of all input geometries expanded by buffer_m."""
    union = unary_union(geometries)
    return union.buffer(buffer_m)


def _clip_and_save(
    gdf: gpd.GeoDataFrame,
    extent: shapely.Geometry,
    output_path: Path,
    layer_name: str,
) -> int:
    """Clip gdf to extent, save as GeoPackage, return feature count."""
    # Filter to features that intersect extent, then clip polygons/lines
    idx = gdf.geometry.intersects(extent)
    clipped = gdf[idx].copy()

    if not clipped.empty:
        geom_types = clipped.geometry.geom_type.unique()
        if any(
            t in geom_types
            for t in ("Polygon", "MultiPolygon", "LineString", "MultiLineString")
        ):
            # Fix any invalid geometries before clipping (e.g. bad winding order)
            invalid = ~clipped.geometry.is_valid
            if invalid.any():
                print(f"  Fixing {invalid.sum()} invalid geometries in {layer_name}")
                clipped.loc[invalid, clipped.geometry.name] = clipped.loc[
                    invalid, clipped.geometry.name
                ].make_valid()
            clipped = clipped.clip(extent)

    if clipped.empty:
        print(f"  WARNING: no features within extent for {layer_name}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    clipped.to_file(output_path, layer=layer_name, driver="GPKG")
    return len(clipped)


def _normalise_crs(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf.crs is None:
        return gdf.set_crs(_CRS_BNG)
    if gdf.crs.to_epsg() != 27700:
        return gdf.to_crs(_CRS_BNG)
    return gdf


def _export_lookups(sqlite_path: Path, output_dir: Path) -> None:
    """Copy lookup tables from production SQLite to a minimal fixtures SQLite."""
    if not sqlite_path.exists():
        print(f"Skipping lookups: not found at {sqlite_path}")
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "lookups.sqlite"

    src = sqlite3.connect(sqlite_path)
    dst = sqlite3.connect(output_path)

    for table in ("WwTw_lookup", "rates_lookup"):
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table}", src)  # noqa: S608
            df.to_sql(table, dst, if_exists="replace", index=False)
            print(f"  {table}: {len(df)} rows → lookups/lookups.sqlite")
        except Exception as e:
            print(f"  Warning: could not export {table}: {e}")

    src.close()
    dst.close()


@app.command()
def main(
    buffer: float = typer.Option(
        1000.0,
        help="Buffer in metres to add around the union of test input extents",
    ),
    output_dir: Path = typer.Option(
        _FIXTURES_DIR,
        help="Output directory for fixture GeoPackages and lookups",
    ),
) -> None:
    """Extract reference data clipped to test input extents.

    Reads source file paths from scripts/.env.local (same as load_data.py).
    Writes clipped GeoPackages and lookup SQLite to tests/data/fixtures/.

    Re-run this script whenever test input geometries change or reference data
    is updated, then commit the updated fixtures and regenerate regression
    baselines with: make update-regression-baseline
    """
    settings = ScriptSettings()

    print(
        f"Collecting test input geometries from {_TEST_INPUTS_DIR.relative_to(_PROJECT_ROOT)}..."
    )
    geometries = _collect_test_input_geometries(_TEST_INPUTS_DIR)
    if not geometries:
        typer.secho(
            "No input geometries found — nothing to clip against",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(1)
    print(f"Total: {len(geometries)} geometries")

    extent = _compute_clip_extent(geometries, buffer)
    minx, miny, maxx, maxy = extent.bounds
    print(
        f"\nClip extent ({buffer:.0f}m buffer): E{minx:.0f}–{maxx:.0f}, N{miny:.0f}–{maxy:.0f}"
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {output_dir}\n")

    # Layers: (fixture_name, source_path, source_layer_name_or_None)
    layers: list[tuple[str, Path, str | None]] = [
        ("wwtw_catchments", settings.wwtw_shapefile_path, None),
        ("lpa_boundaries", settings.lpa_shapefile_path, None),
        ("nn_catchments", settings.nn_catchment_shapefile_path, None),
        ("subcatchments", settings.subcatchment_shapefile_path, None),
        (
            "gcn_risk_zones",
            settings.gcn_risk_zones_gdb_path,
            settings.gcn_risk_zones_layer,
        ),
        ("gcn_ponds", settings.gcn_ponds_gdb_path, settings.gcn_ponds_layer),
        ("edp_edges", settings.edp_edges_gdb_path, settings.edp_edges_layer),
        (
            "edp_boundaries",
            settings.edp_boundary_gpkg_path,
            settings.edp_boundary_layer,
        ),
        (
            "edp_excluded_areas",
            settings.edp_excluded_areas_gpkg_path,
            settings.edp_excluded_areas_layer,
        ),
        (
            "coefficient_layer",
            settings.coefficient_gpkg_path,
            settings.coefficient_layer,
        ),
    ]

    for layer_name, source_path, source_layer in layers:
        if not source_path.exists():
            typer.secho(
                f"Skipping {layer_name}: not found at {source_path}",
                fg=typer.colors.YELLOW,
            )
            continue

        print(f"Processing {layer_name}...")
        gdf = (
            gpd.read_file(source_path, layer=source_layer)
            if source_layer
            else gpd.read_file(source_path)
        )
        gdf = _normalise_crs(gdf)
        count = _clip_and_save(
            gdf, extent, output_dir / f"{layer_name}.gpkg", layer_name
        )
        print(f"  → {layer_name}.gpkg  ({count} features)")

    print("\nExporting lookup tables...")
    _export_lookups(settings.lookup_database_path, output_dir / "lookups")

    typer.secho(f"\nFixtures written to {output_dir}", fg=typer.colors.GREEN, bold=True)
    print("\nNext steps:")
    print(
        "  1. git add tests/data/fixtures/ && git commit -m 'chore: update test fixtures'"
    )
    print("  2. Regenerate regression baselines:")
    print("       make update-regression-baseline")
    print(
        "  3. git add tests/data/expected/ && git commit -m 'chore: update regression baselines'"
    )


if __name__ == "__main__":
    app()
