#!/usr/bin/env python

"""Calculate a nutrient (or GCN) assessment directly, in-process.

No API server, no S3, no SQS — connects straight to PostGIS and calls
run_assessment() the same way the SQS consumer eventually does, just
synchronously. Only requires the database (DB_* config in
scripts/.env.local, same as the other scripts here).

Usage:
    uv run python scripts/nutrient_from_wkt.py --example
    uv run python scripts/nutrient_from_wkt.py --wkt "POLYGON ((...))"
    uv run python scripts/nutrient_from_wkt.py --example --type gcn
    uv run python scripts/nutrient_from_wkt.py --example --dwellings 25 --name "Test Site"
"""

import json
import logging
import sys

import geopandas as gpd
import typer
from shapely import wkt as shapely_wkt

sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent))

from app.assess._geometry import inject_job_fields  # noqa: E402
from app.repositories.engine import create_db_engine  # noqa: E402
from app.repositories.repository import Repository  # noqa: E402
from app.runner.runner import run_assessment  # noqa: E402
from scripts.settings import db_settings  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Run a nutrient/GCN assessment directly against PostGIS, no API/SQS.",
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


def _resolve_wkt(wkt: str | None, example: bool) -> str:
    if example:
        logger.info("Using built-in example polygon (Norfolk Broads, EPSG:27700)")
        return _EXAMPLE_WKT
    if not wkt:
        typer.echo("Error: provide --wkt or --example.", err=True)
        raise typer.Exit(1)
    return wkt


def _wkt_to_gdf(wkt_str: str, crs: str) -> gpd.GeoDataFrame:
    try:
        geometry = shapely_wkt.loads(wkt_str)
    except Exception as exc:
        typer.echo(f"Error: invalid WKT: {exc}", err=True)
        raise typer.Exit(1) from exc

    gdf = gpd.GeoDataFrame(geometry=[geometry], crs=crs)
    if gdf.crs and gdf.crs.to_epsg() != 27700:
        gdf = gdf.to_crs(_CRS_BNG)
    return gdf


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
    example: bool = typer.Option(False, "--example", "-e"),
    assessment_type: str = typer.Option("nutrient", "--type", "-t"),
    crs: str = typer.Option(_CRS_BNG, "--crs"),
    dwelling_type: str = typer.Option("house", "--dwelling-type", "-d"),
    dwellings: int = typer.Option(10, "--dwellings", "-n", min=1),
    name: str = typer.Option("Test Development", "--name"),
    job_id: str = typer.Option("local-script-run", "--job-id"),
):
    """Run an assessment directly against the database and print results as JSON."""
    wkt_str = _resolve_wkt(wkt, example)
    gdf = _wkt_to_gdf(wkt_str, crs)
    gdf = inject_job_fields(gdf, job_id, name, dwelling_type, dwellings)

    settings = db_settings()
    engine = create_db_engine(settings)
    repository = Repository(engine)

    logger.info(
        "Running %s assessment against %s@%s/%s",
        assessment_type,
        settings.user,
        settings.host,
        settings.database,
    )

    try:
        dataframes = run_assessment(
            assessment_type=assessment_type,
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


if __name__ == "__main__":
    sys.exit(app())
