# nrf-impact-assessor

This is work-in-progress. See [To Do List](./TODO.md)

- [nrf-impact-assessor](#nrf-impact-assessor)
  - [Requirements](#requirements)
    - [Python](#python)
    - [Environment Variable Configuration](#environment-variable-configuration)
    - [Linting and Formatting](#linting-and-formatting)
      - [Running Ruff](#running-ruff)
      - [Pre-commit Hooks](#pre-commit-hooks)
      - [VS Code Configuration](#vs-code-configuration)
      - [Ruff Configuration](#ruff-configuration)
    - [Docker](#docker)
  - [Makefile](#makefile)
  - [Local development](#local-development)
    - [Setup \& Configuration](#setup--configuration)
    - [Development](#development)
      - [Option 1: Startup script (preferred)](#option-1-startup-script-preferred)
      - [Option 2: Taskipy command](#option-2-taskipy-command)
      - [Docker Compose (manual)](#docker-compose-manual)
      - [Monitoring Stack (opt-in)](#monitoring-stack-opt-in)
    - [Testing](#testing)
  - [API endpoints](#api-endpoints)
  - [Custom Cloudwatch Metrics](#custom-cloudwatch-metrics)
  - [Pipelines](#pipelines)
    - [Dependabot](#dependabot)
    - [SonarCloud](#sonarcloud)
  - [Licence](#licence)
    - [About the licence](#about-the-licence)

## Requirements

### Python

Please install python `>= 3.13` and `pipx` in your environment. This template uses [uv](https://github.com/astral-sh/uv) to manage the environment and dependencies.

```python
# install uv via pipx
pipx install uv

# sync dependencies
uv sync

# source python venv
source .venv/bin/activate

# install the pre-commit hooks
pre-commit install
```

This opinionated template uses the [`Fast API`](https://fastapi.tiangolo.com/) Python API framework.

### Environment Variable Configuration

The application uses Pydantic's `BaseSettings` for configuration management in `app/config.py`, automatically mapping environment variables to configuration fields.

In CDP, environment variables and secrets need to be set using CDP conventions.  See links below:
- [CDP App Config](https://github.com/DEFRA/cdp-documentation/blob/main/how-to/config.md)
- [CDP Secrets](https://github.com/DEFRA/cdp-documentation/blob/main/how-to/secrets.md)

For local development - see [instructions below](#local-development).

### Linting and Formatting

This project uses [Ruff](https://github.com/astral-sh/ruff) for linting and formatting Python code.

#### Running Ruff

To run Ruff from the command line:

```bash
# Run linting with auto-fix
uv run ruff check . --fix

# Run formatting
uv run ruff format .
```

#### Pre-commit Hooks

This project uses [pre-commit](https://pre-commit.com/) to run linting and formatting checks automatically before each commit.

The pre-commit configuration is defined in `.pre-commit-config.yaml`

To set up pre-commit hooks:

```bash
# Set up the git hooks
pre-commit install
```

To run the hooks manually on all files:

```bash
pre-commit run --all-files
```

#### VS Code Configuration

For the best development experience, configure VS Code to use Ruff:

1. Install the [Ruff extension](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff) for VS Code
2. Configure your VS Code settings (`.vscode/settings.json`):

```json
{
    "editor.formatOnSave": true,
    "editor.codeActionsOnSave": {
        "source.fixAll.ruff": "explicit",
        "source.organizeImports.ruff": "explicit"
    },
    "ruff.lint.run": "onSave",
    "[python]": {
        "editor.defaultFormatter": "charliermarsh.ruff",
        "editor.formatOnSave": true,
        "editor.codeActionsOnSave": {
            "source.fixAll.ruff": "explicit",
            "source.organizeImports.ruff": "explicit"
        }
    }
}
```

This configuration will:

- Format your code with Ruff when you save a file
- Fix linting issues automatically when possible
- Organize imports according to isort rules

#### Ruff Configuration

Ruff is configured in the `.ruff.toml` file

### Docker

This repository uses Docker throughout its lifecycle i.e. both for local development and the environments. A benefit of this is that environment variables & secrets are managed consistently throughout the lifecycle.

See the `Dockerfile` and `compose.yml` for details.

## Makefile

A `Makefile` is provided for common development tasks. Run `make help` to see all available targets.

| Command | Description |
|:---|:---|
| `make help` | Show all available targets |
| `make test` | Run unit tests (`tests/` and `app/`) |
| `make lint` | Run Ruff linter |
| `make format` | Format code and auto-fix lint issues |
| `make build` | Build the service Docker container |
| `make up` | Start core services (detached) |
| `make down` | Stop core services |
| `make logs` | Tail service container logs |
| `make rebuild` | Rebuild and restart the service container |
| `make health` | Check the `/health` endpoint |
| `make monitoring-up` | Start all services including monitoring stack |
| `make monitoring-down` | Stop all services including monitoring stack |
| `make monitoring-logs` | Tail monitoring stack logs (TimescaleDB, Vector, Grafana) |

## Local development

### Setup & Configuration

Follow the convention below for environment variables and secrets in local development.

**Note** that it does not use `.env` or `python-dotenv` as this is not the convention in the CDP environment.

**Environment variables:** `compose/aws.env`.

**Secrets:** `compose/secrets.env`. You need to create this, as it's excluded from version control.

**Libraries:** Ensure the python virtual environment is configured and libraries are installed using `uv sync`, [as above](#python)

**Pre-Commit Hooks:** Ensure you install the pre-commit hooks, as above

See [Docs/environment.md](./Docs/environment.md) for a full reference of all environment variables.

### Development

There are two ways to run the app locally.

Both approaches inject `GIT_HASH` so the `/version` endpoint reports the current commit.

#### Option 1: Startup script (preferred)

The recommended way to run locally. This script checks Docker is running, starts dependent services, loads env files and secrets, then starts the app with hot-reload:

```bash
./scripts/start_dev_server.sh
```

The service will then run on `http://localhost:8085`.

#### Option 2: Taskipy command

Runs only the FastAPI application directly (no Docker, no dependent services). Use this when LocalStack, MongoDB and PostGIS are already running separately:

```bash
uv run task dev
```

#### Docker Compose (manual)

You can also manage Docker Compose services directly:

```bash
make up        # start core services (LocalStack, MongoDB, PostGIS)
make logs      # tail service logs
make rebuild   # rebuild and restart after code changes
```

Or directly:

```bash
docker compose --profile service up --build
```

#### Monitoring Stack (opt-in)

The monitoring services (TimescaleDB, Vector, Grafana) are defined in a separate `compose.monitoring.yml` and are not started by default. To include them:

```bash
make monitoring-up     # start core + monitoring services
make monitoring-logs   # tail TimescaleDB, Vector, Grafana logs
make monitoring-down   # stop everything
```

Once running, Grafana is available at `http://localhost:3000` (admin/admin).

### Testing

Ensure the python virtual environment is configured and libraries are installed using `uv sync`, [as above](#python)

Testing follows the [FastApi documented approach](https://fastapi.tiangolo.com/tutorial/testing/); using pytest & starlette.

```bash
make test      # or: uv run pytest tests/ app/ -v
```

## API endpoints

| Endpoint             | Description                    |
| :------------------- | :----------------------------- |
| `GET: /docs`         | Automatic API Swagger docs     |
| `GET: /health`       | Health check endpoint          |
| `GET: /example/test` | Simple example endpoint        |
| `GET: /example/db`   | Database query example         |
| `GET: /example/http` | HTTP client example            |

## Reference data reload

Reference/spatial tables can be reloaded from S3 `pg_dump` files at runtime,
without a redeploy, via an authenticated background job.

1. Publish the gzipped per-table dumps and a `manifest.json` to S3, bumping the
   manifest's `data_version`. The manifest maps each table to its dump key:

   ```json
   {
     "data_version": "20260603_120000",
     "tables": {
       "nn_catchments": "public_nn_catchments_20260603_120000.sql.gz"
     }
   }
   ```

2. Enable and configure the endpoint via env vars:
   `DATA_SYNC_ENABLED=true`, `DATA_SYNC_S3_BUCKET`, `DATA_SYNC_S3_PREFIX`
   (optional), and `DATA_SYNC_AUTH_TOKEN`.

3. Trigger a reload and poll for status (auth via the `X-Data-Sync-Token`
   header). A reload runs only when the manifest `data_version` differs from the
   last success, or with `?force=true`:

   ```bash
   # Trigger (202 Accepted, returns a run_id)
   curl -X POST "$BASE_URL/admin/data-sync?force=false" \
     -H "X-Data-Sync-Token: $TOKEN"
   # or: make data-sync-trigger TOKEN=$TOKEN [FORCE=true]

   # Poll status
   curl "$BASE_URL/admin/data-sync/<run_id>" -H "X-Data-Sync-Token: $TOKEN"
   ```

Each run replaces every listed table transactionally (truncate, `COPY`) and is
recorded in `data_sync_run` / `data_load_history`. A partial unique index allows
only one run in flight at a time (a concurrent trigger returns `409`).

## Custom Cloudwatch Metrics

Uses the [aws embedded metrics library](https://github.com/awslabs/aws-embedded-metrics-python). An example can be found in `metrics.py`

In order to make this library work in the environments, the environment variable `AWS_EMF_ENVIRONMENT=local` is set in the app config. This tells the library to use the local cloudwatch agent that has been configured in CDP, and uses the environment variables set up in CDP `AWS_EMF_AGENT_ENDPOINT`, `AWS_EMF_LOG_GROUP_NAME`, `AWS_EMF_LOG_STREAM_NAME`, `AWS_EMF_NAMESPACE`, `AWS_EMF_SERVICE_NAME`

## Pipelines

### Dependabot

Dependabot is configured in [.github/dependabot.yml](.github/dependabot.yml) to check for updates weekly (Mondays, 08:30 London time) for:

- **Python** packages (`pip` / `pyproject.toml`) — direct dependencies only
- **Docker** base images (`Dockerfile`, `Dockerfile.vector`)

### SonarCloud

Instructions for setting up SonarCloud can be found in [sonar-project.properties](./sonar-project.properties)

## Licence

THIS INFORMATION IS LICENSED UNDER THE CONDITIONS OF THE OPEN GOVERNMENT LICENCE found at:

<http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3>

The following attribution statement MUST be cited in your products and applications when using this information.

> Contains public sector information licensed under the Open Government license v3

### About the licence

The Open Government Licence (OGL) was developed by the Controller of Her Majesty's Stationery Office (HMSO) to enable
information providers in the public sector to license the use and re-use of their information under a common open
licence.

It is designed to encourage use and re-use of information freely and flexibly, with only a few conditions.
