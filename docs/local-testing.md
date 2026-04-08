# Local Testing Guide

This guide covers running the impact assessor locally against LocalStack (SNS/SQS) and a real or mocked `nrf-backend` for callbacks. Two entry points are available depending on how much of the pipeline you want to exercise.

---

## Overview

| Entry point | What it tests | External deps |
|---|---|---|
| `POST /test/assess` | Assessment logic only (DB, spatial calcs, calculators) | Postgres |
| `POST /test/enqueue` / `make sns-publish` | Full pipeline: SNS → SQS → consumer → orchestrator → runner → nrf-backend PATCH callback | Postgres, LocalStack, consumer, (optionally) nrf-backend |

Both `/test/*` endpoints are only mounted when `API_TESTING_ENABLED=true` and are never available in production.

---

## Stack

`compose.yml` provides everything except `nrf-backend` itself:

- `postgres` — PostGIS with reference data (port `5434` on host)
- `localstack` — SQS + SNS, gateway remapped to host port `4568`
- `mongodb` — tile metadata
- `service` — the impact assessor; runs `python -m app.consumer`, which spawns the API server (`:8085`) as a subprocess and polls SQS in the main process

Start everything:

```bash
make up                               # brings up all services
docker compose logs -f service        # watch assessor
```

---

## The SNS → SQS pipeline

`compose/start-localstack.sh` provisions a single SQS queue (`nrf-impact-assessment-jobs`), a single SNS topic (`nrf-quote-estimate-request`), and subscribes the queue to the topic. The consumer unwraps the SNS envelope automatically (`app/aws/sqs.py`).

### Trigger a job

```bash
make sns-publish                              # default sample payload
make sns-publish PAYLOAD=scripts/mine.json    # custom payload
make sqs-send                                 # skip SNS, push straight to SQS
make sqs-peek                                 # non-consuming peek
make sqs-depth                                # visible + in-flight counts
make sqs-purge                                # clear the queue
```

The default payload lives at `scripts/sample_quote_payload.json` and matches the quote schema the real `nrf-backend` publishes: `reference` (`NRF-######`), `boundaryGeojson`, `developmentTypes`, `residentialBuildingCount`, `email`, and an `edps` list used by the callback.

> The LocalStack gateway is remapped to host port `4568` to avoid clashing with `nrf-backend`'s own LocalStack. Override with `make sns-publish LOCALSTACK_URL=http://localhost:4566` if needed.

### Watch it flow through

```bash
docker compose logs -f service | grep -E \
  'SQS consumer started|Received job message|Processing job|PATCH http'
```

Expected sequence for a successful run:

```
SQS consumer started, polling for jobs...
Received job message: NRF-000001
Processing job: NRF-000001
Step 1: Loading geometry from SQS message
Step 2: Validating inline geometry
Step 3: Injecting job data
Step 4: Running nutrient assessment via runner
Job NRF-000001 completed successfully in 2.34s
Sent assessment results to nrf-backend for quote NRF-000001
PATCH http://host.docker.internal:3001/quotes/NRF-000001 succeeded (HTTP 200)
```

---

## nrf-backend callback

When the assessment finishes, `JobOrchestrator._send_results_callback` fires a `PATCH {BACKEND_BASE_URL}/quotes/{reference}` with the assessment results. The callback is only attempted if **all three** are true:

1. `BACKEND_BASE_URL` is set on the service container
2. The job has a `reference`
3. The job has a non-empty `edps` list

`compose.yml` defaults `BACKEND_BASE_URL` to `http://host.docker.internal:3001`, which points at a real `nrf-backend` running on the host.

### Pointing at a running nrf-backend

Start nrf-backend locally so it listens on `0.0.0.0:3001` (loopback-only binds won't be reachable from the container). Its Swagger is at <http://localhost:3001/docs/index.html> — confirm the exact PATCH route there. If the route sits under a prefix (e.g. `/api/v1/quotes/{reference}`), override the base URL to include it:

```bash
BACKEND_BASE_URL=http://host.docker.internal:3001/api/v1 make up
```

### Verifying the callback was received

In the service logs:

```bash
docker compose logs -f service | grep 'PATCH http'
```

You should see `PATCH .../quotes/NRF-000001 succeeded (HTTP 200)`. A 4xx response is logged with the full error body; retries only happen on 5xx or transport errors.

### Unit-testing the callback

To assert "the PATCH controller receives the reference" without running containers, mock `BackendClient.patch_quote`:

```python
from unittest.mock import MagicMock
from app.orchestrator import JobOrchestrator
from app.models.enums import AssessmentType

backend = MagicMock()
orch = JobOrchestrator(aws_config=..., repository=..., backend_client=backend)
orch.process_job(job_with_edps, AssessmentType.NUTRIENT)

backend.patch_quote.assert_called_once()
assert backend.patch_quote.call_args.args[0] == "NRF-000001"
```

---

## `/test/assess` — assessment logic only

Synchronous, no SQS, no callback. Returns results immediately in the HTTP response. Good for iterating on the assessment code itself.

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

Or via the CLI wrapper:

```bash
uv run python scripts/test_wkt.py assess --example
uv run python scripts/test_wkt.py assess --example --type gcn
uv run python scripts/test_wkt.py assess --example --dwellings 50 --dwelling-type apartment
```

---

## `/test/enqueue` — WKT-driven SQS push

Takes a WKT polygon, wraps it in an SNS-shaped `ImpactAssessmentJob` (with a generated `NRF-######` reference), and pushes it directly onto the SQS queue — bypassing SNS. The consumer picks it up on its next poll.

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

Or via the CLI wrapper:

```bash
uv run python scripts/test_wkt.py enqueue --example
```

Note: `/test/enqueue` does not populate `edps`, so the backend callback will **not** fire for jobs enqueued this way. Use `make sns-publish` when you want to exercise the full pipeline including the PATCH callback.

---

## WKT input notes

- Default CRS is `EPSG:27700` (British National Grid). Coordinates must match.
- To provide WGS84 coordinates, pass `"crs": "EPSG:4326"` — the server reprojects automatically.
- Only single-polygon WKT is supported (`POLYGON`). Multi-polygons are not tested.

Example polygons:

| Location | WKT (EPSG:27700) |
|---|---|
| Norfolk Broads | `POLYGON ((620000 310000, 620500 310000, 620500 310500, 620000 310500, 620000 310000))` |
| River Wye area | `POLYGON ((352000 212000, 352500 212000, 352500 212500, 352000 212500, 352000 212000))` |

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| No `SQS consumer started` line in service logs | Container is running `app.main` instead of `app.consumer` | Rebuild: `make rebuild` (the Dockerfile `CMD` should be `-m app.consumer`) |
| `receive_message` hits empty URL / nothing picked up | `AWS_SQS_QUEUE_URL` not set on the service | Check `docker compose exec service env \| grep AWS_SQS_QUEUE_URL` |
| `Received job message: None` | Job missing `reference` — pydantic dropped it | Confirm payload matches `^NRF-\d{6}$` |
| Job runs but no PATCH is sent | `backend_client` not initialised, `reference` missing, or `edps` empty | Look for `Backend callback enabled:` at startup; check the payload includes `edps` |
| `PATCH ... failed with HTTP 404` | Path prefix mismatch | Set `BACKEND_BASE_URL=http://host.docker.internal:3001/<prefix>` |
| `PATCH ... connection refused` | nrf-backend bound to `127.0.0.1` only | Bind to `0.0.0.0` or run nrf-backend inside the same docker network |
| `ValueError: Required columns missing from input: ['shape_area']` | Orchestrator injected wrong column | Should be `shape_area`, not `area_m2` (app/orchestrator.py) |
| `404 Not Found` on `/test/*` | `API_TESTING_ENABLED=false` | Set it to `true` (already true in `compose.yml` for local dev) |

---

## How the pipeline fits together

```
make sns-publish
  ↓
LocalStack SNS (nrf-quote-estimate-request)
  ↓  (subscription)
LocalStack SQS (nrf-impact-assessment-jobs)
  ↓  (long-polled by consumer)
SqsConsumer.receive_messages()
  ↓  (SNS envelope unwrapped)
ImpactAssessmentJob.model_validate()
  ↓
JobOrchestrator.process_job()
  → _process_inline_geometry()        # shape → GeoDataFrame → validate → inject
  → run_assessment()                  # nutrient/gcn calculators
  → _send_results_callback()          # PATCH /quotes/{reference}
          ↓
   nrf-backend (host:3001)
```

The geometry travels inline in the SQS message body. Any failure along the chain surfaces in the service logs.
