# Environment Variables

This document describes all environment variables used by the application and its local development stack.

> **Secrets** — `compose/secrets.env` is gitignored and must never be committed. Run `make secrets-init` to generate it with a random password. Do not hand-edit placeholder values into this file.

---

## Application (`app/config.py`)

| Variable | Default | Description |
|---|---|---|
| `HOST` | `127.0.0.1` | Address the server binds to |
| `PORT` | `8086` | Port the server listens on. Overridden to `8085` by `compose.yml` and `scripts/start_dev_server.sh` locally |
| `PYTHON_ENV` | `None` | Deployment environment. Set to `production` in CDP; enables startup validation of required secrets |
| `LOG_CONFIG` | `None` | Path to the JSON logging config file |
| `MONGO_URI` | `None` | MongoDB connection URI. Unset/blank skips the startup connection entirely — the app has no other mongo usage, so local runs need no mongo container |
| `MONGO_DATABASE` | `nrf-impact-assessor` | MongoDB database name |
| `MONGO_TRUSTSTORE` | `TRUSTSTORE_CDP_ROOT_CA` | TLS truststore for MongoDB |
| `AWS_ENDPOINT_URL` | `None` | Override AWS endpoint (set to LocalStack URL locally). Also read by `AWSConfig` — see the AWS section below |
| `HTTP_PROXY` | `None` | Outbound HTTP proxy URL |
| `ENABLE_METRICS` | `false` | Enable CloudWatch EMF metrics emission |
| `TRACING_HEADER` | `x-cdp-request-id` | Header name used for request tracing |
| `WORKERS` | `1` | Number of uvicorn worker processes |
| `IMPACT_ASSESSOR_API_KEY` | `""` | Required value of the `x-api-key` header on every non-public endpoint. **Required in production** — startup fails without it |

---

## API Server (`app/config.py` — `ApiServerConfig`)

| Variable | Default | Description |
|---|---|---|
| `API_HOST` | `0.0.0.0` | Host interface for the API server |
| `API_PORT` | `8085` | Port for the API server (a separate setting from `PORT`) |
| `API_TRACING_HEADER` | `x-cdp-request-id` | HTTP header name for CDP distributed tracing |
| `API_TESTING_ENABLED` | `false` | Mount `/test/*` endpoints for local development. **Never enable in production.** See [local-testing.md](local-testing.md). |
| `API_ASSESS_JOB_TTL_SECONDS` | `3600` | How long `/assess` job results are kept in memory before cleanup (seconds) |
| `API_MAX_UPLOAD_BYTES` | `2097152` | Maximum size of an uploaded geometry file (2 MB). `/compare-boundaries` accepts a JSON body of twice this |

---

## Tile Server (`app/config.py` — `TileServerConfig`)

| Variable | Default | Description |
|---|---|---|
| `TILE_CACHE_MAX_SIZE` | `1000` | Maximum number of tiles held in the in-process LRU cache |
| `TILE_CACHE_TTL_SECONDS` | `3600` | Seconds before a cached tile is considered stale |
| `TILE_VERSION_TTL_SECONDS` | `300` | Seconds before the cached layer version is re-queried from the database |
| `TILE_MIN_ZOOM` | `0` | Minimum zoom level accepted by `GET /tiles/...` (inclusive) |
| `TILE_MAX_ZOOM` | `22` | Maximum zoom level accepted by `GET /tiles/...` (inclusive) |

---

## Database (`app/config.py` — `DatabaseSettings`)

Connection settings for the PostGIS reference database.

| Variable | Default | Description |
|---|---|---|
| `DB_HOST` | `localhost` | Database host |
| `DB_PORT` | `5432` | Database port |
| `DB_DATABASE` | `nrf_impact` | Database name |
| `DB_USER` | `postgres` | Database user |
| `DB_IAM_AUTHENTICATION` | `true` | Use IAM authentication for RDS. Automatically defaults to `false` when `DB_HOST` is a loopback address and the variable is not explicitly set |
| `DB_LOCAL_PASSWORD` | `""` | Static password for local development. Unset uses trust auth |
| `DB_SSL_MODE` | `require` | SSL mode for database connections (`require`, `verify-ca`, `verify-full`) |
| `DB_RDS_TRUSTSTORE` | `RDS_ROOT_CA` | Name of the `TRUSTSTORE_*` env var containing the RDS CA cert |
| `DB_KEEPALIVE_WARM_SLOTS` | `3` | Maximum idle pool slots the background keepalive refreshes per tick; bounds the per-replica connection floor |
| `DB_KEEPALIVE_INTERVAL_SECONDS` | `240` | How often the keepalive refreshes pooled connections. Must stay below the 600 s pool recycle window, or `0` to disable — startup fails otherwise |

---

## Backend (`app/config.py` — `BackendConfig`)

Outbound calls to nrf-backend (assessment result callbacks, levy PATCHes).

| Variable | Default | Description |
|---|---|---|
| `BACKEND_BASE_URL` | `""` | Base URL for the nrf-backend API |
| `BACKEND_API_KEY` | `""` | Value of the `x-api-key` header sent on outbound calls to nrf-backend. **Required in production** |
| `BACKEND_CALLBACK_TIMEOUT` | `30` | HTTP timeout in seconds for callbacks |
| `BACKEND_CALLBACK_MAX_RETRIES` | `3` | Max retry attempts for failed callbacks |

---

## Assessment (`app/config.py` — `AssessmentConfig` and nested configs)

Business rules for nutrient impact assessment.

| Variable | Default | Description |
|---|---|---|
| `IAT_PRECAUTIONARY_BUFFER_PERCENT` | `20.0` | Precautionary buffer added to total nutrient impacts (%) |
| `IAT_FALLBACK_WWTW_ID` | `141` | WwTW ID used for developments outside modelled catchments |
| `IAT_OCCUPANCY_RATE` | `2.11` | EDP-wide occupancy rate (people per dwelling) |
| `IAT_WATER_USAGE_L_PER_PERSON_DAY` | `110.0` | EDP-wide water usage (litres per person per day) |

Greenspace adjustment (`GreenspaceConfig`):

| Variable | Default | Description |
|---|---|---|
| `GS_THRESHOLD_AREA_HA` | `2.5` | Development area (ha) above which greenspace is assumed |
| `GS_GREENSPACE_PERCENT` | `10.0` | Percentage of the development assumed to be greenspace |
| `GS_NITROGEN_COEFF` | `3.0` | Greenspace nitrogen coefficient (kg/ha/year) |
| `GS_PHOSPHORUS_COEFF` | `0.2` | Greenspace phosphorus coefficient (kg/ha/year) |

SuDS mitigation (`SuDsConfig`):

| Variable | Default | Description |
|---|---|---|
| `SUDS_THRESHOLD_AREA_HA` | `2.5` | Development area (ha) at or above which SuDS is applied |
| `SUDS_CAPTURE_PERCENT` | `100.0` | Share of runoff entering SuDS (%) |
| `SUDS_REMOVAL_RATE_PERCENT` | `15.0` | SuDS nutrient removal rate (%) |

---

## GCN assessment (`app/config.py` — `GcnConfig`)

Great Crested Newt impact assessment.

| Variable | Default | Description |
|---|---|---|
| `GCN_BUFFER_DISTANCE_M` | `250` | Buffer distance around the RLB (metres) |
| `GCN_POND_BUFFER_DISTANCE_M` | `250` | Buffer distance around ponds (metres) |
| `GCN_MERGE_DISTANCE_M` | `500` | Sites within this distance create a single buffer (metres) |
| `GCN_PRECISION_GRID_SIZE` | `0.0001` | Coordinate precision grid size (metres; 0.1 mm matches ArcGIS XY resolution) |
| `GCN_TARGET_CRS` | `EPSG:27700` | British National Grid coordinate reference system |

---

## Spatial cache (`app/config.py` — `SpatialCacheConfig`)

In-memory cache for spatial query results.

| Variable | Default | Description |
|---|---|---|
| `SPATIAL_CACHE_MAX_SIZE` | `256` | Max number of cached query results |
| `SPATIAL_CACHE_TTL_SECONDS` | `3600` | Cache entry TTL in seconds |

---

## Aerial imagery proxy (`app/config.py` — `AerialProxyConfig`)

The `GET /aerial_proxy/{z}/{x}/{y}` endpoint.

| Variable | Default | Description |
|---|---|---|
| `AERIAL_PROXY_BASE_URL` | `""` | Upstream WMTS GetTile URL without the tile coordinates. `TILEMATRIX`/`TILEROW`/`TILECOL` are set per request |
| `AERIAL_PROXY_WMS_BASE_URL` | `""` | Optional WMS GetMap URL without `CRS`/`BBOX`/`WIDTH`/`HEIGHT`, served from zoom 14 up. Imagery is requested in EPSG:27700 and reprojected to Web Mercator with OSTN15. Left empty, the WMTS serves every zoom |
| `AERIAL_PROXY_TIMEOUT_SECONDS` | `10` | HTTP timeout for upstream tile requests |
| `AERIAL_PROXY_MAX_TILE_BYTES` | `2097152` | Maximum accepted upstream tile response body (2 MB) — a 256 px raster tile is well under this |
| `AERIAL_PROXY_CACHE_MAX_SIZE` | `500` | Max number of cached upstream tiles |
| `AERIAL_PROXY_CACHE_MAX_BYTES` | `134217728` | Total payload budget for the tile cache (128 MB); caps resident memory per worker |
| `AERIAL_PROXY_CACHE_TTL_SECONDS` | `3600` | Cache entry TTL in seconds |
| `AERIAL_PROXY_MISSING_CACHE_TTL_SECONDS` | `300` | How long a "no imagery here" result is cached, so panning over uncovered regions does not re-hit the upstream |
| `AERIAL_PROXY_ERROR_CACHE_TTL_SECONDS` | `15` | How long an upstream failure is cached; kept short so the proxy recovers quickly |

---

## Data sync (`app/config.py` — `DataSyncConfig`)

S3-triggered reference-data reload (`/admin/data-sync`). See [README](../README.md#reference-data-reload).

| Variable | Default | Description |
|---|---|---|
| `DATA_SYNC_ENABLED` | `false` | Mount the `/admin/data-sync` endpoints |
| `DATA_SYNC_S3_BUCKET` | `""` | Bucket holding the dump objects |
| `DATA_SYNC_S3_PREFIX` | `""` | Key prefix for dump objects |
| `DATA_SYNC_AUTH_TOKEN` | `""` | Shared token required to trigger a reload (sent as `x-data-sync-token`) |
| `DATA_SYNC_LOCK_KEY` | `728191` | Postgres advisory lock key for reload runs |
| `DATA_SYNC_TABLES` | 11 reference tables | Fallback allow-list (JSON list); the manifest `tables` map is authoritative. See `app/config.py` for the list |

---

## Debug output (`app/config.py` — `DebugConfig`)

**Local development only — never enable in production.**

| Variable | Default | Description |
|---|---|---|
| `DEBUG_OUTPUT` | `false` | Write intermediate assessment artefacts to disk |
| `DEBUG_OUTPUT_DIR` | `$TMPDIR/iat-debug` | Directory the artefacts are written to |

---

## AWS (`app/config.py` — `AWSConfig`)

Application-side AWS resource configuration for the ECS worker.

| Variable | Default | Description |
|---|---|---|
| `AWS_ACCOUNT_ID` | `""` | AWS account ID |
| `AWS_REGION` | `eu-west-2` | AWS region |
| `AWS_SQS_QUEUE_URL` | `""` | SQS queue URL for assessment jobs |
| `AWS_ENDPOINT_URL` | `None` | Override AWS endpoint for LocalStack — same variable as listed under Application above |

---

## AWS / LocalStack (`compose/aws.env`)

Used by both the application and the LocalStack container. Safe to commit — values are for local development only.

| Variable | Local value | Description |
|---|---|---|
| `AWS_REGION` | `eu-west-2` | AWS region |
| `AWS_DEFAULT_REGION` | `eu-west-2` | AWS default region |
| `AWS_ACCESS_KEY_ID` | `test` | Dummy key for LocalStack |
| `AWS_SECRET_ACCESS_KEY` | `test` | Dummy secret for LocalStack |
| `AWS_EMF_ENVIRONMENT` | `local` | Tells EMF library to use the local agent |
| `AWS_EMF_AGENT_ENDPOINT` | `tcp://127.0.0.1:25888` | CloudWatch agent endpoint |
| `AWS_EMF_LOG_GROUP_NAME` | — | CloudWatch log group name |
| `AWS_EMF_LOG_STREAM_NAME` | — | CloudWatch log stream name |
| `AWS_EMF_NAMESPACE` | — | CloudWatch metrics namespace |
| `AWS_EMF_SERVICE_NAME` | — | Service name reported in metrics |
| `AWS_EMF_SERVICE_TYPE` | `python-backend-service` | Service type reported in metrics |

---

## Secrets (`compose/secrets.env`)

This file is **gitignored** and must never be committed. It is generated from `compose/secrets.template`.

**First-time setup — generate a random password automatically:**

```bash
make secrets-init
```

This creates `compose/secrets.env` with a cryptographically random 32-character password derived from `openssl rand`. It is a no-op if the file already exists, so it is safe to run repeatedly.

To regenerate (e.g. after a password rotation):

```bash
rm compose/secrets.env && make secrets-init
```

| Variable | Description |
|---|---|
| `METRICS_DB_PASSWORD` | Password for the TimescaleDB `metrics` user. Used by TimescaleDB, Vector, and Grafana. Must not be empty or set to the placeholder value `change_me` — `make monitoring-up` enforces this. |

> **`API_TESTING_ENABLED` does not belong here.** It is a non-secret application flag already set to `"true"` in `compose.yml` for the `service` container. See the API Server section above.

---

## Monitoring Stack

The monitoring services (TimescaleDB, Vector, Grafana) live in a separate `compose.monitoring.yml` file and are **opt-in**. To start them alongside the core services:

```bash
make monitoring-up      # start core + monitoring
make monitoring-logs    # tail monitoring logs
make monitoring-down    # stop everything
```

The environment variables below are set directly in `compose.monitoring.yml` and reference the secrets above.

| Variable | Service | Description |
|---|---|---|
| `POSTGRES_DB` | `timescaledb` | Database name (`metrics`) |
| `POSTGRES_USER` | `timescaledb` | Database user (`metrics`) |
| `POSTGRES_PASSWORD` | `timescaledb` | See `METRICS_DB_PASSWORD` above (secret) |
| `DOCKER_API_VERSION` | `vector` | Docker daemon API version for container stats collection |
| `GF_SECURITY_ADMIN_USER` | `grafana` | Grafana admin username |
| `GF_SECURITY_ADMIN_PASSWORD` | `grafana` | Grafana admin password — change before any shared use |

---

## Performance Notes

### Uvicorn reload disabled in Docker

Previously the app used `PYTHON_ENV=development` (set by the old `defradigital/python-development` base image) to enable uvicorn's built-in `reload=True` file watcher, which continuously polls the filesystem and causes ~30% idle CPU usage inside the container.

This has been removed. Hot-reload is instead handled by Docker Compose's `develop.watch` feature (configured in `compose.yml`), which syncs file changes from the host into the container without the polling overhead. Press `w` after `docker compose --profile service up` to enable watch mode.
