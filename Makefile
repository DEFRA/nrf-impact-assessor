.PHONY: help test test-integration test-regression update-regression-baseline check-migration-parity lint format build up down logs rebuild health monitoring-up monitoring-down monitoring-logs load-data load-data-sample load-data-layer load-data-lookup db-migrate db-rollback db-backup db-backup-schema db-backup-globals db-backup-tables db-restore db-restore-tables secrets-init _check-secrets sns-publish sqs-send sqs-peek sqs-depth sqs-purge

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Development
# ---------------------------------------------------------------------------
TEST_ENV = DB_IAM_AUTHENTICATION=false DB_HOST=localhost DB_PORT=5434

test: ## Run unit tests only (integration and regression excluded by default)
	$(TEST_ENV) uv run pytest tests/ app/ -v

test-integration: ## Run integration tests against test_nrf_impact DB on port 5434
	$(TEST_ENV) uv run pytest tests/integration/ -v -m integration

REGRESSION_ENV = DB_IAM_AUTHENTICATION=false DB_HOST=localhost DB_PORT=5434

test-regression: ## Run regression tests against production DB on port 5434
	$(REGRESSION_ENV) uv run pytest tests/regression/ -v -m regression

update-regression-baseline: ## Regenerate nutrient regression baselines from PostGIS (run then commit the CSVs)
	$(REGRESSION_ENV) PYTHONPATH=. uv run python scripts/update_regression_baselines.py

check-migration-parity: ## Check every Alembic migration has a matching Liquibase changeset
	python scripts/check_migration_parity.py

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

db-backup-tables: ## Per-table backup — schema grants + one .sql.gz per table in nrf_reference
	@mkdir -p $(BACKUP_DIR)
	@schema_out="$(BACKUP_DIR)/nrf_reference_schema_$(TS).sql.gz"; \
	echo "  nrf_reference schema → $$schema_out"; \
	docker exec $(DB_CONTAINER) pg_dump -U $(DB_USER) --format=plain \
		--no-password --schema-only -n nrf_reference $(DB_NAME) | gzip > "$$schema_out"
	@for table in $(DB_TABLES); do \
		name=$$(echo $$table | tr '.' '_'); \
		out="$(BACKUP_DIR)/$${name}_$(TS).sql.gz"; \
		echo "  $$table → $$out"; \
		docker exec $(DB_CONTAINER) pg_dump -U $(DB_USER) --format=plain \
			--no-password --data-only -t $$table $(DB_NAME) | gzip > "$$out"; \
	done
	@echo "Per-table backups written to $(BACKUP_DIR)"

db-restore: ## Restore from .sql.gz backup: make db-restore BACKUP_FILE=./backups/foo.sql.gz
	@test -n "$(BACKUP_FILE)" || (echo "ERROR: set BACKUP_FILE=<path>"; exit 1)
	@test -f "$(BACKUP_FILE)" || (echo "ERROR: file not found: $(BACKUP_FILE)"; exit 1)
	zcat $(BACKUP_FILE) | docker exec -i $(DB_CONTAINER) psql -U $(DB_USER) $(DB_NAME)
	@echo "Restore complete from $(BACKUP_FILE)"

db-restore-tables: ## Restore per-table backup: apply schema grants then table data from BACKUP_DIR
	@test -n "$(BACKUP_DIR)" || (echo "ERROR: set BACKUP_DIR=<path>"; exit 1)
	@schema_file=$$(ls -t $(BACKUP_DIR)/nrf_reference_schema_*.sql.gz 2>/dev/null | head -1); \
	if [ -z "$$schema_file" ]; then \
		echo "ERROR: no nrf_reference_schema_*.sql.gz found in $(BACKUP_DIR)"; exit 1; \
	fi; \
	echo "Restoring schema grants from $$schema_file"; \
	zcat "$$schema_file" | docker exec -i $(DB_CONTAINER) psql -U $(DB_USER) $(DB_NAME)
	@for table in $(DB_TABLES); do \
		name=$$(echo $$table | tr '.' '_'); \
		f=$$(ls -t $(BACKUP_DIR)/$${name}_*.sql.gz 2>/dev/null | head -1); \
		if [ -z "$$f" ]; then echo "  WARNING: no backup found for $$table — skipping"; continue; fi; \
		echo "  $$table ← $$f"; \
		zcat "$$f" | docker exec -i $(DB_CONTAINER) psql -U $(DB_USER) $(DB_NAME); \
	done
	@echo "Per-table restore complete from $(BACKUP_DIR)"

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

extract-fixtures: ## Clip reference layers to test input extents → tests/data/fixtures/ (requires .env.local)
	PYTHONPATH=. uv run python scripts/extract_test_fixtures.py

load-fixtures: ## Load committed fixture data into nrf_impact DB (no .env.local required)
	$(LOAD_DATA_ENV) uv run python scripts/load_data.py --fixtures-dir tests/data/fixtures/

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

# ---------------------------------------------------------------------------
# LocalStack SNS / SQS (host gateway is remapped to 4568 in compose.yml)
# ---------------------------------------------------------------------------
LOCALSTACK_URL ?= http://localhost:4568
AWS_LOCAL       = AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=eu-west-2 aws --endpoint-url=$(LOCALSTACK_URL)
SNS_TOPIC_ARN   = arn:aws:sns:eu-west-2:000000000000:nrf-quote-estimate-request
SQS_QUEUE_URL   = http://localhost:4568/000000000000/nrf-impact-assessment-jobs
SAMPLE_PAYLOAD  = scripts/sample_quote_payload.json

sns-publish: ## Publish sample quote payload to SNS (wrapped → SQS). Override: PAYLOAD=path/to.json
	$(AWS_LOCAL) sns publish \
		--topic-arn $(SNS_TOPIC_ARN) \
		--message file://$(or $(PAYLOAD),$(SAMPLE_PAYLOAD))

BACKEND_URL ?= http://localhost:3001

sns-publish-real: ## POST a quote to nrf-backend, then publish to SNS using the real reference
	@ref=$$(curl -s -X POST $(BACKEND_URL)/quotes \
		-H "Content-Type: application/json" \
		-d '{"boundaryEntryType":"draw","developmentTypes":["housing"],"residentialBuildingCount":25,"email":"developer@example.com"}' \
		| python3 -c "import sys, json; print(json.load(sys.stdin)['reference'])"); \
	if [ -z "$$ref" ]; then echo "Failed to create quote on $(BACKEND_URL)"; exit 1; fi; \
	echo "Created quote: $$ref"; \
	python3 -c "import json; p=json.load(open('$(SAMPLE_PAYLOAD)')); p['reference']='$$ref'; print(json.dumps(p))" > /tmp/nrf_quote_with_ref.json; \
	$(AWS_LOCAL) sns publish \
		--topic-arn $(SNS_TOPIC_ARN) \
		--message file:///tmp/nrf_quote_with_ref.json; \
	echo "Published SNS message for quote $$ref"

sqs-send: ## Send payload directly to SQS (bypasses SNS envelope). Override: PAYLOAD=path/to.json
	$(AWS_LOCAL) sqs send-message \
		--queue-url $(SQS_QUEUE_URL) \
		--message-body file://$(or $(PAYLOAD),$(SAMPLE_PAYLOAD))

sqs-peek: ## Peek at queue without consuming (visibility-timeout=0)
	$(AWS_LOCAL) sqs receive-message \
		--queue-url $(SQS_QUEUE_URL) \
		--visibility-timeout 0 \
		--max-number-of-messages 10

sqs-depth: ## Show approximate queue depth
	$(AWS_LOCAL) sqs get-queue-attributes \
		--queue-url $(SQS_QUEUE_URL) \
		--attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible

sqs-purge: ## Purge all messages from the queue
	$(AWS_LOCAL) sqs purge-queue --queue-url $(SQS_QUEUE_URL)
