#!/usr/bin/env python

"""Calculate a nutrient assessment directly, in-process.

The site boundary can be given as WKT, a GeoJSON file, a shapefile, or a
zipped shapefile.

No API server, no S3, no SQS — connects straight to PostGIS and calls
run_assessment() the same way the SQS consumer eventually does, just
synchronously. Only requires the database (DB_* config in
scripts/.env.local, same as the other scripts here).

Usage:
    uv run python scripts/local_nutrient_assessment.py --example
    uv run python scripts/local_nutrient_assessment.py --wkt "POLYGON ((...))"
    uv run python scripts/local_nutrient_assessment.py --file site.geojson
    uv run python scripts/local_nutrient_assessment.py --file site.shp
    uv run python scripts/local_nutrient_assessment.py --file site.zip
    uv run python scripts/local_nutrient_assessment.py --example --dwellings 25 --name "Test Site"

A file with a ``Dwellings`` field uses each feature's own dwelling count
instead of --dwellings. Results are also written as CSV to --out-dir: one
file per feature named after its ``ID`` field when the file has one,
otherwise a single ``<job-id>.csv``.
"""

import json
import logging
import re
import sys
import zipfile
from pathlib import Path, PurePosixPath

import geopandas as gpd
import typer
from shapely import wkt as shapely_wkt

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.assess._geometry import inject_job_fields  # noqa: E402
from app.repositories.engine import create_db_engine  # noqa: E402
from app.repositories.repository import Repository  # noqa: E402
from app.runner.runner import run_assessment  # noqa: E402
from scripts.settings import db_settings  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Run a nutrient assessment directly against PostGIS, no API/SQS.",
    add_completion=False,
)

_EXAMPLE_WKT = (
    "POLYGON (("
    "620000 310000, "
    "620500 310000, "
    "620500 310500, "
    "620000 310500, "
    "620000 310000"
    "))"
)

_CRS_BNG = "EPSG:27700"

_FILE_SUFFIXES = (".geojson", ".json", ".shp", ".zip")

# Attribute fields carried over from an input file (matched case-insensitively).
_ID_FIELD = "id"
_DWELLINGS_FIELD = "dwellings"


def _resolve_wkt(wkt: str | None, example: bool) -> str:
    if example:
        logger.info("Using built-in example polygon (Norfolk Broads, EPSG:27700)")
        return _EXAMPLE_WKT
    if not wkt:
        typer.echo("Error: provide --wkt, --file or --example.", err=True)
        raise typer.Exit(1)
    return wkt


def _wkt_to_gdf(wkt_str: str, crs: str) -> gpd.GeoDataFrame:
    try:
        geometry = shapely_wkt.loads(wkt_str)
    except Exception as exc:
        typer.echo(f"Error: invalid WKT: {exc}", err=True)
        raise typer.Exit(1) from exc

    gdf = gpd.GeoDataFrame(geometry=[geometry], crs=crs)
    return _to_bng(gdf)


def _file_to_gdf(path: Path, crs: str) -> gpd.GeoDataFrame:
    """Read a GeoJSON, shapefile or zipped shapefile; ``crs`` is used only if the file has none."""
    if not path.is_file():
        typer.echo(f"Error: file not found: {path}", err=True)
        raise typer.Exit(1)
    suffix = path.suffix.lower()
    if suffix not in _FILE_SUFFIXES:
        typer.echo(
            f"Error: unsupported file format {suffix}. Use {', '.join(_FILE_SUFFIXES)}.",
            err=True,
        )
        raise typer.Exit(1)

    source = _zip_member_uri(path) if suffix == ".zip" else str(path)
    try:
        gdf = gpd.read_file(source)
    except Exception as exc:
        typer.echo(f"Error: failed to read {path}: {exc}", err=True)
        raise typer.Exit(1) from exc

    if gdf.empty:
        typer.echo(f"Error: {path} contains no features.", err=True)
        raise typer.Exit(1)
    if gdf.crs is None:
        logger.warning("%s has no CRS; assuming %s", path, crs)
        gdf = gdf.set_crs(crs)
    logger.info("Read %d feature(s) from %s", len(gdf), path)
    return _to_bng(_keep_attribute_fields(gdf))


def _keep_attribute_fields(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Keep geometry plus any ID/Dwellings fields, renamed to lower case."""
    by_lower = {str(col).lower(): col for col in gdf.columns if col != "geometry"}
    keep = {
        by_lower[field]: field
        for field in (_ID_FIELD, _DWELLINGS_FIELD)
        if field in by_lower
    }
    gdf = gdf[[*keep, "geometry"]].rename(columns=keep)
    if _DWELLINGS_FIELD in gdf.columns:
        dwellings = gdf[_DWELLINGS_FIELD]
        if dwellings.isna().any() or (dwellings < 1).any():
            typer.echo(
                "Error: Dwellings field must be a positive number for every feature.",
                err=True,
            )
            raise typer.Exit(1)
        gdf[_DWELLINGS_FIELD] = dwellings.astype(int)
        logger.info("Using per-feature dwellings from the file's Dwellings field")
    if _ID_FIELD in gdf.columns:
        gdf[_ID_FIELD] = gdf[_ID_FIELD].astype(str)
    return gdf


def _zip_member_uri(path: Path) -> str:
    """Point GDAL at the .shp/.geojson inside a zip, skipping macOS ``__MACOSX``/``._*`` entries."""
    try:
        with zipfile.ZipFile(path) as zf:
            names = zf.namelist()
    except zipfile.BadZipFile as exc:
        typer.echo(f"Error: {path} is not a valid zip: {exc}", err=True)
        raise typer.Exit(1) from exc

    members = [
        PurePosixPath(n)
        for n in names
        if "__MACOSX" not in PurePosixPath(n).parts
        and not PurePosixPath(n).name.startswith("._")
    ]
    for wanted in (".shp", ".geojson", ".json"):
        matches = [m for m in members if m.suffix.lower() == wanted]
        if matches:
            return f"zip://{path.resolve()}!{matches[0]}"
    typer.echo(f"Error: {path} must contain a .shp or .geojson file.", err=True)
    raise typer.Exit(1)


def _to_bng(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf.crs and gdf.crs.to_epsg() != 27700:
        gdf = gdf.to_crs(_CRS_BNG)
    return gdf


def _apply_job_fields(
    gdf: gpd.GeoDataFrame,
    job_id: str,
    name: str,
    dwelling_type: str,
    dwellings: int,
) -> gpd.GeoDataFrame:
    """Inject job fields, keeping any per-feature ID/Dwellings read from the file."""
    file_fields = gdf[
        [c for c in (_ID_FIELD, _DWELLINGS_FIELD) if c in gdf.columns]
    ].copy()
    gdf = inject_job_fields(gdf, job_id, name, dwelling_type, dwellings)
    for column in file_fields.columns:
        gdf[column] = file_fields[column]
    return gdf


def _csv_filename(stem: str) -> str:
    """Make an ID safe to use as a filename (IDs like ``GNLP0337/0326`` contain slashes)."""
    safe = re.sub(r"[^\w.\-]+", "_", stem).strip("._")
    return f"{safe or 'result'}.csv"


def _write_csv(
    dataframes: dict, out_dir: Path, job_id: str, has_file_ids: bool
) -> None:
    """Write the impact summary as CSV: one file per ID when the input had IDs."""
    summary = dataframes["impact_summary"]
    out_dir.mkdir(parents=True, exist_ok=True)

    if not has_file_ids:
        path = out_dir / _csv_filename(job_id)
        summary.to_csv(path, index=False)
        logger.info("Wrote %d row(s) to %s", len(summary), path)
        return

    used: set[str] = set()
    for feature_id, rows in summary.groupby(_ID_FIELD, sort=False):
        filename = _csv_filename(str(feature_id))
        stem = filename.removesuffix(".csv")
        suffix = 1
        while filename in used:
            suffix += 1
            filename = f"{stem}_{suffix}.csv"
        if suffix > 1:
            logger.warning("Filename clash for ID %r; writing %s", feature_id, filename)
        used.add(filename)
        rows.to_csv(out_dir / filename, index=False)
    logger.info("Wrote %d CSV file(s) to %s", len(used), out_dir)


def _print_json(dataframes: dict) -> None:
    results = {}
    for key, df in dataframes.items():
        if hasattr(df, "geometry") and "geometry" in df.columns:
            results[key] = df.drop(columns=["geometry"]).to_dict(orient="records")
        else:
            results[key] = df.to_dict(orient="records")
    typer.echo(json.dumps(results, indent=2, default=str))


@app.command()
def assess(
    wkt: str | None = typer.Option(None, "--wkt", "-w"),
    file: Path | None = typer.Option(
        None, "--file", "-f", help="GeoJSON (.geojson/.json), shapefile (.shp) or zip."
    ),
    example: bool = typer.Option(False, "--example", "-e"),
    crs: str = typer.Option(_CRS_BNG, "--crs"),
    dwelling_type: str = typer.Option("house", "--dwelling-type", "-d"),
    dwellings: int = typer.Option(
        10,
        "--dwellings",
        "-n",
        min=1,
        help="Used when the input file has no Dwellings field.",
    ),
    name: str = typer.Option("Test Development", "--name"),
    job_id: str = typer.Option("local-script-run", "--job-id"),
    out_dir: Path = typer.Option(
        Path(__file__).parent / "csv_export",
        "--out-dir",
        "-o",
        help="Directory for CSV results.",
    ),
):
    """Run a nutrient assessment against the database; print JSON and write CSV."""
    if sum(bool(x) for x in (wkt, file, example)) > 1:
        typer.echo("Error: use only one of --wkt, --file or --example.", err=True)
        raise typer.Exit(1)
    if file:
        gdf = _file_to_gdf(file, crs)
    else:
        gdf = _wkt_to_gdf(_resolve_wkt(wkt, example), crs)
    has_file_ids = _ID_FIELD in gdf.columns
    gdf = _apply_job_fields(gdf, job_id, name, dwelling_type, dwellings)

    settings = db_settings()
    engine = create_db_engine(settings)
    repository = Repository(engine)

    logger.info(
        "Running nutrient assessment against %s@%s/%s",
        settings.user,
        settings.host,
        settings.database,
    )

    try:
        dataframes = run_assessment(
            assessment_type="nutrient",
            rlb_gdf=gdf,
            metadata={"unique_ref": job_id},
            repository=repository,
        )
    except (KeyError, ValueError) as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1) from exc
    finally:
        engine.dispose()

    _print_json(dataframes)
    _write_csv(dataframes, out_dir, job_id, has_file_ids)


if __name__ == "__main__":
    sys.exit(app())
