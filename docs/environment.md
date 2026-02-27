# Environment Variables

This document describes all environment variables used by the application and its local development stack.

> **Secrets** — variables marked `secret` must never be committed to version control. Set them in `compose/secrets.env`, which is excluded from git.

---

## Application (`app/config.py`)

| Variable | Default | Description |
|---|---|---|
| `PYTHON_ENV` | `None` | Runtime environment name. Set to `development` by the `defradigital/python-development` base image. **Do not** use this to control uvicorn reload — Docker Compose `develop.watch` handles hot-reload externally. |
| `HOST` | `127.0.0.1` | Address the server binds to |
| `PORT` | `8086` | Port the server listens on |
| `LOG_CONFIG` | `None` | Path to the JSON logging config file |
| `MONGO_URI` | `None` | MongoDB connection URI |
| `MONGO_DATABASE` | `nrf-impact-assessor` | MongoDB database name |
| `MONGO_TRUSTSTORE` | `TRUSTSTORE_CDP_ROOT_CA` | TLS truststore for MongoDB |
| `AWS_ENDPOINT_URL` | `None` | Override AWS endpoint (set to LocalStack URL locally) |
| `HTTP_PROXY` | `None` | Outbound HTTP proxy URL |
| `ENABLE_METRICS` | `false` | Enable CloudWatch EMF metrics emission |
| `TRACING_HEADER` | `x-cdp-request-id` | Header name used for request tracing |

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

This file is **gitignored** and must be created manually. Copy the template below and fill in real values.

```bash
# compose/secrets.env — do not commit
METRICS_DB_PASSWORD=<your-password>
```

| Variable | Description |
|---|---|
| `METRICS_DB_PASSWORD` | Password for the TimescaleDB `metrics` user. Used by TimescaleDB, Vector, and Grafana. |

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

The `defradigital/python-development` base image sets `PYTHON_ENV=development`. Previously, the app used this to enable uvicorn's built-in `reload=True` file watcher, which continuously polls the filesystem and causes ~30% idle CPU usage inside the container.

This has been removed. Hot-reload is instead handled by Docker Compose's `develop.watch` feature (configured in `compose.yml`), which syncs file changes from the host into the container without the polling overhead. Press `w` after `docker compose --profile service up` to enable watch mode.