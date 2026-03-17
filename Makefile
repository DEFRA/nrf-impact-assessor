.PHONY: help test lint format build up down logs rebuild health monitoring-up monitoring-down monitoring-logs load-data load-data-sample load-data-layer load-data-lookup db-migrate db-rollback db-backup db-backup-schema db-backup-globals db-backup-tables db-restore secrets-init _check-secrets

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Development
# ---------------------------------------------------------------------------
test: ## Run unit tests
	uv run pytest tests/ app/ -v

lint: ## Run linter
	uv run ruff check .

format: ## Format code and auto-fix lint issues
	uv run ruff format .
	uv run ruff check . --fix --exit-zero

# ---------------------------------------------------------------------------
# Database backup / restore
# ---------------------------------------------------------------------------
DB_CONTAINER  = nrf-postgis
DB_NAME       = nrf_impact
DB_USER       = postgres
BACKUP_DIR   ?= ./backups
TS             = $(shell date +%Y%m%d_%H%M%S)
BACKUP_FILE  ?= $(BACKUP_DIR)/$(DB_NAME)_$(TS).sql.gz

# Tables to include in per-table backup (schema-qualified)
DB_TABLES = \
	nrf_reference.spatial_layer \
	nrf_reference.coefficient_layer \
	nrf_reference.edp_boundary_layer \
	nrf_reference.lookup_table

db-backup: ## Full backup — schema, data, custom types and grants (.sql.gz)
	@mkdir -p $(BACKUP_DIR)
	docker exec $(DB_CONTAINER) pg_dump -U $(DB_USER) --format=plain \
		--no-password $(DB_NAME) | gzip > $(BACKUP_FILE)
	@echo "Backup written to $(BACKUP_FILE)"

db-backup-schema: ## Schema-only backup — tables, enums, indexes, grants (.sql.gz, no data)
	@mkdir -p $(BACKUP_DIR)
	docker exec $(DB_CONTAINER) pg_dump -U $(DB_USER) --format=plain \
		--schema-only --no-password $(DB_NAME) \
		| gzip > $(BACKUP_DIR)/$(DB_NAME)_schema_$(TS).sql.gz
	@echo "Schema backup written to $(BACKUP_DIR)"

db-backup-globals: ## Cluster-level roles and grants (.sql.gz via pg_dumpall)
	@mkdir -p $(BACKUP_DIR)
	docker exec $(DB_CONTAINER) pg_dumpall -U $(DB_USER) --globals-only \
		| gzip > $(BACKUP_DIR)/$(DB_NAME)_globals_$(TS).sql.gz
	@echo "Globals backup written to $(BACKUP_DIR)"

db-backup-tables: ## Per-table backup — one .sql.gz per table in nrf_reference
	@mkdir -p $(BACKUP_DIR)
	@for table in $(DB_TABLES); do \
		name=$$(echo $$table | tr '.' '_'); \
		out="$(BACKUP_DIR)/$${name}_$(TS).sql.gz"; \
		echo "  $$table → $$out"; \
		docker exec $(DB_CONTAINER) pg_dump -U $(DB_USER) --format=plain \
			--no-password -t $$table $(DB_NAME) | gzip > "$$out"; \
	done
	@echo "Per-table backups written to $(BACKUP_DIR)"

db-restore: ## Restore from .sql.gz backup: make db-restore BACKUP_FILE=./backups/foo.sql.gz
	@test -n "$(BACKUP_FILE)" || (echo "ERROR: set BACKUP_FILE=<path>"; exit 1)
	@test -f "$(BACKUP_FILE)" || (echo "ERROR: file not found: $(BACKUP_FILE)"; exit 1)
	zcat $(BACKUP_FILE) | docker exec -i $(DB_CONTAINER) psql -U $(DB_USER) $(DB_NAME)
	@echo "Restore complete from $(BACKUP_FILE)"

# ---------------------------------------------------------------------------
# Database migrations
# ---------------------------------------------------------------------------
DB_MIGRATE_ENV = DB_IAM_AUTHENTICATION=false DB_HOST=localhost DB_PORT=5434

db-migrate: ## Apply all pending Alembic migrations
	$(DB_MIGRATE_ENV) uv run alembic upgrade head

db-rollback: ## Rollback the last Alembic migration
	$(DB_MIGRATE_ENV) uv run alembic downgrade -1

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
LOAD_DATA_ENV = PYTHONPATH=. DB_IAM_AUTHENTICATION=false DB_HOST=localhost DB_PORT=5434

load-data: ## Load all reference data into PostGIS (destructive)
	$(LOAD_DATA_ENV) uv run python scripts/load_data.py

load-data-sample: ## Load sample data only (100 features per layer)
	$(LOAD_DATA_ENV) uv run python scripts/load_data.py --sample

load-data-layer: ## Load a specific layer e.g. make load-data-layer LAYER=wwtw_catchments
	$(LOAD_DATA_ENV) uv run python scripts/load_data.py --layer $(LAYER)

load-data-lookup: ## Load a specific lookup e.g. make load-data-lookup LOOKUP=wwtw_lookup
	$(LOAD_DATA_ENV) uv run python scripts/load_data.py --lookup $(LOOKUP)

# ---------------------------------------------------------------------------
# Docker
# ---------------------------------------------------------------------------
build: ## Build service container
	docker compose build service

up: ## Start all services
	docker compose --profile service up -d

down: ## Stop all services
	docker compose down

logs: ## Tail service logs
	docker compose logs -f service

rebuild: build ## Rebuild and restart service
	docker compose --profile service up -d service

# ---------------------------------------------------------------------------
# Secrets
# ---------------------------------------------------------------------------
SECRETS_FILE = compose/secrets.env
SECRETS_TEMPLATE = compose/secrets.template
PLACEHOLDER = change_me

secrets-init: ## Create compose/secrets.env with a generated password (skips if already exists)
	@if [ -f $(SECRETS_FILE) ]; then \
		echo "$(SECRETS_FILE) already exists — skipping. Delete it first to regenerate."; \
	else \
		password=$$(openssl rand -base64 32 | tr -d '/+=' | head -c 32); \
		sed "s/METRICS_DB_PASSWORD=.*/METRICS_DB_PASSWORD=$$password/" $(SECRETS_TEMPLATE) > $(SECRETS_FILE); \
		echo "$(SECRETS_FILE) created with a generated password."; \
	fi

_check-secrets:
	@if [ ! -f $(SECRETS_FILE) ]; then \
		echo "ERROR: $(SECRETS_FILE) not found. Run 'make secrets-init' first."; \
		exit 1; \
	fi
	@if grep -q "METRICS_DB_PASSWORD=$(PLACEHOLDER)" $(SECRETS_FILE); then \
		echo "ERROR: $(SECRETS_FILE) still contains the placeholder password '$(PLACEHOLDER)'."; \
		echo "Run 'make secrets-init' to generate a secure password."; \
		exit 1; \
	fi
	@if grep -q "METRICS_DB_PASSWORD=$$" $(SECRETS_FILE); then \
		echo "ERROR: METRICS_DB_PASSWORD is empty in $(SECRETS_FILE)."; \
		echo "Run 'make secrets-init' to generate a secure password."; \
		exit 1; \
	fi

# ---------------------------------------------------------------------------
# Monitoring
# ---------------------------------------------------------------------------
MONITORING_COMPOSE = docker compose -f compose.yml -f compose.monitoring.yml

monitoring-up: _check-secrets ## Start all services including monitoring stack
	$(MONITORING_COMPOSE) --profile service up -d

monitoring-down: ## Stop all services including monitoring stack
	$(MONITORING_COMPOSE) down

monitoring-logs: ## Tail monitoring stack logs
	$(MONITORING_COMPOSE) logs -f timescaledb vector grafana

# ---------------------------------------------------------------------------
# Test endpoints
# ---------------------------------------------------------------------------
BASE_URL ?= http://0.0.0.0:8085

health: ## Check health endpoint
	curl -s $(BASE_URL)/health | python -m json.tool

db-check: ## Check database tables and row counts (requires API_TESTING_ENABLED=true)
	curl -s $(BASE_URL)/test/db | python -m json.tool