# Local Testing Guide

This guide explains how to test assessment logic locally without uploading files or relying on a frontend. Two approaches are available, both using WKT polygon strings as input.

---

## Overview

| Endpoint | What it tests | External deps |
|---|---|---|
| `POST /test/assess` | Assessment logic (DB queries, spatial calcs, calculators) | DB only |
| `POST /test/enqueue` | Full SQS pipeline (S3 → SQS → consumer → orchestrator → runner) | DB + LocalStack + running consumer |

Both endpoints are **only available when `API_TESTING_ENABLED=true`** and are never mounted in production.

---

## Prerequisites

### For `/test/assess` (DB only)

- PostGIS database running with reference data loaded
- API server running with `API_TESTING_ENABLED=true`

```bash
# Start the database
docker compose up db

# Start the API server with testing enabled
API_TESTING_ENABLED=true uv run python -m app.main
```

### For `/test/enqueue` (full pipeline)

All of the above, plus:

- LocalStack running with S3 bucket and SQS queue provisioned
- SQS consumer running

```bash
# Start LocalStack + DB
docker compose up db localstack

# Start the consumer (polls SQS and processes jobs)
docker compose --profile worker up worker

# Start the API server with testing enabled
API_TESTING_ENABLED=true uv run python -m app.main
```

---

## Option A — CLI script (recommended)

`scripts/test_wkt.py` wraps both endpoints with a simple command-line interface. No boto3 or file handling required.

### Direct assessment

```bash
# Use the built-in example polygon (Norfolk Broads area)
uv run python scripts/test_wkt.py assess --example

# Provide your own WKT
uv run python scripts/test_wkt.py assess \
    --wkt "POLYGON ((620000 310000, 620500 310000, 620500 310500, 620000 310500, 620000 310000))"

# GCN assessment
uv run python scripts/test_wkt.py assess --example --type gcn

# Custom development parameters
uv run python scripts/test_wkt.py assess \
    --example \
    --type nutrient \
    --dwelling-type apartment \
    --dwellings 50 \
    --name "My Development"
```

Results are printed as formatted JSON to stdout.

### Full SQS pipeline

```bash
# Enqueue a job and watch the consumer process it
uv run python scripts/test_wkt.py enqueue --example

# Then watch consumer logs:
docker compose logs -f worker
# Look for: Processing job: <job_id>
```

### All options

```bash
uv run python scripts/test_wkt.py --help
uv run python scripts/test_wkt.py assess --help
uv run python scripts/test_wkt.py enqueue --help
```

---

## Option B — curl / HTTP client

### `POST /test/assess`

```bash
curl -s -X POST http://localhost:8085/test/assess \
  -H "Content-Type: application/json" \
  -d '{
    "wkt": "POLYGON ((620000 310000, 620500 310000, 620500 310500, 620000 310500, 620000 310000))",
    "assessment_type": "nutrient",
    "dwelling_type": "house",
    "dwellings": 10,
    "name": "Test Site"
  }' | python -m json.tool
```

**Response:**

```json
{
  "job_id": "...",
  "assessment_type": "nutrient",
  "timing_s": 2.34,
  "results": {
    "nutrient_results": [ { "RLB_ID": 1, "N_Total": 12.5, ... } ]
  }
}
```

### `POST /test/enqueue`

```bash
curl -s -X POST http://localhost:8085/test/enqueue \
  -H "Content-Type: application/json" \
  -d '{
    "wkt": "POLYGON ((620000 310000, 620500 310000, 620500 310500, 620000 310500, 620000 310000))",
    "assessment_type": "nutrient",
    "dwelling_type": "house",
    "dwellings": 10,
    "developer_email": "test@example.com"
  }'
```

**Response (202):**

```json
{
  "job_id": "...",
  "s3_key": "jobs/.../input.geojson",
  "message_id": "...",
  "note": "Consumer will process on next poll. Watch worker logs for: 'Processing job: ...'"
}
```

---

## WKT input notes

- Default CRS is `EPSG:27700` (British National Grid). Coordinates must match.
- To provide WGS84 coordinates (longitude/latitude), pass `"crs": "EPSG:4326"` — the server reprojects automatically.
- Only single-polygon WKT is supported (`POLYGON`). Multi-polygons are not tested.

**Example polygons:**

| Location | WKT (EPSG:27700) |
|---|---|
| Norfolk Broads | `POLYGON ((620000 310000, 620500 310000, 620500 310500, 620000 310500, 620000 310000))` |
| River Wye area | `POLYGON ((352000 212000, 352500 212000, 352500 212500, 352000 212500, 352000 212000))` |

---

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `404 Not Found` on `/test/assess` | Testing endpoints not mounted | Set `API_TESTING_ENABLED=true` and restart the server |
| `400 AWS_S3_INPUT_BUCKET is not configured` | AWS env vars missing | Set `AWS_S3_INPUT_BUCKET`, `AWS_SQS_QUEUE_URL`, `AWS_ENDPOINT_URL` |
| `502 S3 upload failed` | LocalStack not running or bucket not created | Run `docker compose up localstack` |
| `502 SQS send failed` | LocalStack SQS queue not provisioned | Check LocalStack setup and queue URL |
| `500 Assessment failed` | Assessment logic error | Check server logs for the full traceback |
| Connection refused | API server not running | Start with `API_TESTING_ENABLED=true uv run python -m app.main` |

---

## How `/test/enqueue` exercises the production path

```
POST /test/enqueue
  ↓
WKT → GeoDataFrame → GeoJSON bytes
  ↓
S3Client.put_object → LocalStack S3 (s3://nrf-inputs/jobs/{id}/input.geojson)
  ↓
SQSClient.send_message → LocalStack SQS
  ↓  (consumer polls on next cycle)
SqsConsumer.receive_messages()
  ↓
JobOrchestrator.process_job()
  → S3Client.download_geometry_file()
  → GeometryValidator.validate()
  → _inject_job_data()
  → run_assessment()
```

This path is identical to the production flow. Any failure in `orchestrator.py`, `aws/s3.py`, or `aws/sqs.py` will surface in the consumer logs.
