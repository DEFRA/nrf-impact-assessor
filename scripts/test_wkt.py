#!/usr/bin/env python

"""Submit test assessments via the local API test endpoints.

This script calls the /test/* endpoints on a running local API server.
It requires API_TESTING_ENABLED=true on the server.

No boto3, no file upload, no LocalStack setup required for `assess`.
For `enqueue`, LocalStack and the SQS consumer must be running.

Usage:
    # Run assessment directly and print results:
    uv run python scripts/test_wkt.py assess --wkt "POLYGON (...)"

    # Use a predefined example polygon (Broads area):
    uv run python scripts/test_wkt.py assess --example

    # Upload to LocalStack S3 + enqueue to SQS (full pipeline):
    uv run python scripts/test_wkt.py enqueue --wkt "POLYGON (...)"
    uv run python scripts/test_wkt.py enqueue --example

    # GCN assessment type:
    uv run python scripts/test_wkt.py assess --example --type gcn

    # Point at a different server:
    uv run python scripts/test_wkt.py assess --example --base-url http://localhost:8085
"""

import json
import logging
import sys

import httpx
import typer

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Test NRF assessment endpoints using a WKT geometry string.",
    add_completion=False,
)

# A small square polygon in the Norfolk Broads area (EPSG:27700).
# Suitable for nutrient and GCN assessments.
_EXAMPLE_WKT = (
    "POLYGON (("
    "620000 310000, "
    "620500 310000, "
    "620500 310500, "
    "620000 310500, "
    "620000 310000"
    "))"
)

_WKT_HELP = "WKT polygon string in the CRS specified by --crs."
_EXAMPLE_HELP = "Use the built-in example polygon (Norfolk Broads area, EPSG:27700)."
_TYPE_HELP = "Assessment type: 'nutrient' or 'gcn'."
_CRS_HELP = "Coordinate reference system of the WKT (default: EPSG:27700)."
_BASE_URL_HELP = "Base URL of the running API server."


def _resolve_wkt(wkt: str | None, example: bool) -> str:
    if example:
        logger.info("Using built-in example polygon (Norfolk Broads, EPSG:27700)")
        return _EXAMPLE_WKT
    if not wkt:
        typer.echo("Error: provide --wkt or --example.", err=True)
        raise typer.Exit(1)
    return wkt


def _print_json(data: dict) -> None:
    typer.echo(json.dumps(data, indent=2, default=str))


@app.command()
def assess(
    wkt: str | None = typer.Option(None, "--wkt", "-w", help=_WKT_HELP),
    example: bool = typer.Option(False, "--example", "-e", help=_EXAMPLE_HELP),
    assessment_type: str = typer.Option("nutrient", "--type", "-t", help=_TYPE_HELP),
    crs: str = typer.Option("EPSG:27700", "--crs", help=_CRS_HELP),
    dwelling_type: str = typer.Option("house", "--dwelling-type", "-d"),
    dwellings: int = typer.Option(10, "--dwellings", "-n", min=1),
    name: str = typer.Option("Test Development", "--name"),
    base_url: str = typer.Option(
        "http://localhost:8086", "--base-url", help=_BASE_URL_HELP
    ),
):
    """Run an assessment directly and print results as JSON.

    Calls POST /test/assess — synchronous, no S3 or SQS required.
    The API server must be running with API_TESTING_ENABLED=true.
    """
    wkt_str = _resolve_wkt(wkt, example)

    payload = {
        "wkt": wkt_str,
        "crs": crs,
        "assessment_type": assessment_type,
        "dwelling_type": dwelling_type,
        "dwellings": dwellings,
        "name": name,
    }

    url = f"{base_url.rstrip('/')}/test/assess"
    logger.info(
        "POST %s  (assessment_type=%s, dwellings=%d)", url, assessment_type, dwellings
    )

    try:
        response = httpx.post(url, json=payload, timeout=120)
    except httpx.ConnectError as exc:
        typer.echo(
            f"Error: could not connect to {base_url}. "
            "Is the server running with API_TESTING_ENABLED=true?",
            err=True,
        )
        raise typer.Exit(1) from exc

    if response.status_code != 200:
        typer.echo(f"Error {response.status_code}: {response.text}", err=True)
        raise typer.Exit(1)

    result = response.json()
    logger.info(
        "Assessment %s completed in %.2fs",
        result.get("job_id", "?"),
        result.get("timing_s", 0),
    )
    _print_json(result)


@app.command()
def enqueue(
    wkt: str | None = typer.Option(None, "--wkt", "-w", help=_WKT_HELP),
    example: bool = typer.Option(False, "--example", "-e", help=_EXAMPLE_HELP),
    assessment_type: str = typer.Option("nutrient", "--type", "-t", help=_TYPE_HELP),
    crs: str = typer.Option("EPSG:27700", "--crs", help=_CRS_HELP),
    dwelling_type: str = typer.Option("house", "--dwelling-type", "-d"),
    dwellings: int = typer.Option(10, "--dwellings", "-n", min=1),
    name: str = typer.Option("Test Development", "--name"),
    developer_email: str = typer.Option("test@example.com", "--email"),
    base_url: str = typer.Option(
        "http://localhost:8086", "--base-url", help=_BASE_URL_HELP
    ),
):
    """Upload geometry to LocalStack S3 and enqueue an SQS job message.

    Calls POST /test/enqueue — exercises the full SQS pipeline.
    Requires LocalStack running and the SQS consumer active.
    Watch consumer logs for: 'Processing job: <job_id>'.
    """
    wkt_str = _resolve_wkt(wkt, example)

    payload = {
        "wkt": wkt_str,
        "crs": crs,
        "assessment_type": assessment_type,
        "dwelling_type": dwelling_type,
        "dwellings": dwellings,
        "name": name,
        "developer_email": developer_email,
    }

    url = f"{base_url.rstrip('/')}/test/enqueue"
    logger.info(
        "POST %s  (assessment_type=%s, dwellings=%d)", url, assessment_type, dwellings
    )

    try:
        response = httpx.post(url, json=payload, timeout=30)
    except httpx.ConnectError as exc:
        typer.echo(
            f"Error: could not connect to {base_url}. "
            "Is the server running with API_TESTING_ENABLED=true?",
            err=True,
        )
        raise typer.Exit(1) from exc

    if response.status_code not in (200, 202):
        typer.echo(f"Error {response.status_code}: {response.text}", err=True)
        raise typer.Exit(1)

    result = response.json()
    typer.echo(f"\nJob enqueued: {result.get('job_id')}")
    typer.echo(f"S3 key:       {result.get('s3_key')}")
    typer.echo(f"SQS msg ID:   {result.get('message_id')}")
    typer.echo(f"\n{result.get('note')}")
    typer.echo("\nWatch consumer logs:")
    typer.echo("  docker compose logs -f worker")
    typer.echo(f"  Look for: Processing job: {result.get('job_id')}")


if __name__ == "__main__":
    sys.exit(app())
