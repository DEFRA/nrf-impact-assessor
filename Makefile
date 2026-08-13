.PHONY: help test test-integration test-regression update-regression-baseline check-migration-parity lint format build up down logs rebuild health monitoring-up monitoring-down monitoring-logs load-data load-data-sample load-data-layer load-data-lookup fixture-manifest db-migrate db-rollback db-migrate-liquibase db-rollback-liquibase db-backup db-backup-schema db-backup-globals db-backup-tables db-restore db-restore-tables data-sync-trigger secrets-init _check-secrets sns-publish sqs-send sqs-peek sqs-depth sqs-purge

# ---------------------------------------------------------------------------
# Shell
# ---------------------------------------------------------------------------
# Several recipes below are POSIX shell scripts (if/for/case, pipelines,
# $$(...) substitution) that cmd.exe cannot run, and make defaults to cmd.exe
# on Windows. Point it at the sh that ships with Git for Windows / WSL instead;
# make searches PATH for it, so running from Git Bash or WSL just works.
ifeq ($(OS),Windows_NT)
SHELL := sh.exe
.SHELLFLAGS := -c
endif

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Development
# ---------------------------------------------------------------------------
# Host port the compose postgres publishes on; override if it collides with a
# postgres you already run: make test-integration DB_PORT=5433
DB_PORT ?= 5432
DB_HOST ?= localhost
DB_IAM_AUTHENTICATION ?= false

# Exported rather than written as an inline `VAR=value cmd` prefix: that prefix
# is POSIX shell syntax, so on Windows cmd.exe tries to run "DB_HOST=localhost"
# as a program instead of setting a variable. `export` is make's own mechanism
# and reaches the recipe whatever the shell. compose.yml sets its own DB_HOST /
# DB_IAM_AUTHENTICATION for the service container, so exporting these here does
# not leak into the containers; DB_PORT it deliberately reads from the env.
export DB_PORT
export DB_HOST
export DB_IAM_AUTHENTICATION
# The repo root is not an installed package, so scripts/ need it on sys.path.
export PYTHONPATH := .

test: ## Run unit tests only (integration and regression excluded by default)
	uv run pytest tests/ app/ -v

test-integration: ## Run integration tests against the local test_nrf_impact DB
	uv run pytest tests/integration/ -v -m integration

test-regression: ## Run regression tests against the local production-data DB
	uv run pytest tests/regression/ -v -m regression

update-regression-baseline: ## Regenerate nutrient regression baselines from PostGIS (run then commit the CSVs)
	uv run python scripts/update_regression_baselines.py

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
# Part size for split backups (GitHub rejects files over 100MB)
BACKUP_PART_SIZE ?= 100m
# Target used in the generated restore_commands.txt (remote/prod restore)
RESTORE_USER ?= nrf_impact_assessor_ddl
RESTORE_DB   ?= nrf_impact_assessor

# Tables to include in per-table backup (schema-qualified)
DB_TABLES = \
	public.coefficient_layer \
	public.edp_boundary_layer \
	public.edp_excluded_areas \
	public.lookup_table \
	public.wwtw_catchments \
	public.lpa_boundaries \
	public.nn_catchments \
	public.subcatchments \
	public.gcn_risk_zones \
	public.gcn_ponds \
	public.edp_edges

db-tables: ## List public tables with their exact row counts
	@docker exec $(DB_CONTAINER) psql -U $(DB_USER) -d $(DB_NAME) -tA -c \
		"SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename" \
	| while read t; do \
		[ -z "$$t" ] && continue; \
		c=$$(docker exec $(DB_CONTAINER) psql -U $(DB_USER) -d $(DB_NAME) -tAc \
			"SELECT count(*) FROM public.\"$$t\""); \
		printf '  %-30s %12s\n' "$$t" "$$c"; \
	done

db-backup: ## Full backup — schema, data, custom types and grants, split into <100MB parts (.sql.gz.part-*); set SPLIT=0 for a single .sql.gz
	@mkdir -p $(BACKUP_DIR)
	@if [ "$(SPLIT)" = "0" ]; then \
		docker exec $(DB_CONTAINER) pg_dump -U $(DB_USER) --format=plain --no-owner \
			--no-password $(DB_NAME) | gzip > $(BACKUP_FILE); \
		echo "Backup written to $(BACKUP_FILE)"; \
	else \
		docker exec $(DB_CONTAINER) pg_dump -U $(DB_USER) --format=plain --no-owner \
			--no-password $(DB_NAME) | gzip | split -b $(BACKUP_PART_SIZE) - $(BACKUP_FILE).part-; \
		echo "Backup written to $(BACKUP_FILE).part-*"; \
	fi

db-backup-schema: ## Schema-only backup — tables, enums, indexes, grants (.sql.gz, no data)
	@mkdir -p $(BACKUP_DIR)
	docker exec $(DB_CONTAINER) pg_dump -U $(DB_USER) --format=plain --no-owner \
		--schema-only --no-password $(DB_NAME) \
		| gzip > $(BACKUP_DIR)/$(DB_NAME)_schema_$(TS).sql.gz
	@echo "Schema backup written to $(BACKUP_DIR)"

db-backup-globals: ## Cluster-level roles and grants (.sql.gz via pg_dumpall)
	@mkdir -p $(BACKUP_DIR)
	docker exec $(DB_CONTAINER) pg_dumpall -U $(DB_USER) --globals-only \
		| gzip > $(BACKUP_DIR)/$(DB_NAME)_globals_$(TS).sql.gz
	@echo "Globals backup written to $(BACKUP_DIR)"

db-backup-tables: ## Per-table backup — schema grants + one .sql.gz per table in public (large tables split into .part-*); set SPLIT=0 to never split
	@mkdir -p $(BACKUP_DIR)
	@schema_out="$(BACKUP_DIR)/public_schema_$(TS).sql.gz"; \
	echo "  public schema → $$schema_out"; \
	docker exec $(DB_CONTAINER) pg_dump -U $(DB_USER) --format=plain --no-owner \
		--no-password --schema-only -n public $(DB_NAME) | gzip > "$$schema_out"; \
	restore="$(BACKUP_DIR)/restore_commands.txt"; \
	printf 'gunzip -c %s | psql -U $(RESTORE_USER) -d $(RESTORE_DB) -v ON_ERROR_STOP=1\n\n' \
		"$$(basename "$$schema_out")" > "$$restore"
	@restore="$(BACKUP_DIR)/restore_commands.txt"; \
	for table in $(DB_TABLES); do \
		name=$$(echo $$table | tr '.' '_'); \
		out="$(BACKUP_DIR)/$${name}_$(TS).sql.gz"; \
		echo "  $$table → $$out"; \
		if [ "$(SPLIT)" = "0" ]; then \
			docker exec $(DB_CONTAINER) pg_dump -U $(DB_USER) --format=plain --no-owner \
				--no-password --data-only -t $$table $(DB_NAME) | gzip > "$$out"; \
		else \
			docker exec $(DB_CONTAINER) pg_dump -U $(DB_USER) --format=plain --no-owner \
				--no-password --data-only -t $$table $(DB_NAME) | gzip \
				| split -b $(BACKUP_PART_SIZE) - "$$out.part-"; \
			if [ ! -e "$$out.part-ab" ]; then \
				mv "$$out.part-aa" "$$out"; \
			else \
				echo "    split into $$(ls "$$out".part-* | wc -l | tr -d ' ') parts"; \
			fi; \
		fi; \
		base=$$(basename "$$out"); \
		if [ -e "$$out" ]; then decompress="gunzip -c $$base"; \
		else decompress="cat $$base.part-* | gunzip -c"; fi; \
		printf 'psql -U $(RESTORE_USER) -d $(RESTORE_DB) -c "TRUNCATE TABLE %s;" && %s | psql -U $(RESTORE_USER) -d $(RESTORE_DB) -v ON_ERROR_STOP=1\n' \
			"$$table" "$$decompress" >> "$$restore"; \
	done
	@echo "Per-table backups written to $(BACKUP_DIR)"
	@echo "Restore commands written to $(BACKUP_DIR)/restore_commands.txt"

db-restore: ## Restore from backup: make db-restore BACKUP_FILE=./backups/foo.sql.gz (whole file or .part-* splits)
	@test -n "$(BACKUP_FILE)" || (echo "ERROR: set BACKUP_FILE=<path>"; exit 1)
	@if [ -f "$(BACKUP_FILE)" ]; then \
		gzip -dc "$(BACKUP_FILE)" | docker exec -i $(DB_CONTAINER) psql -U $(DB_USER) $(DB_NAME); \
	elif ls $(BACKUP_FILE).part-* >/dev/null 2>&1; then \
		cat $(BACKUP_FILE).part-* | gzip -dc | docker exec -i $(DB_CONTAINER) psql -U $(DB_USER) $(DB_NAME); \
	else \
		echo "ERROR: no backup found at $(BACKUP_FILE) or $(BACKUP_FILE).part-*"; exit 1; \
	fi
	@echo "Restore complete from $(BACKUP_FILE)"

db-restore-tables: ## Restore per-table backup: apply schema grants then table data from BACKUP_DIR
	@test -n "$(BACKUP_DIR)" || (echo "ERROR: set BACKUP_DIR=<path>"; exit 1)
	@schema_file=$$(ls -t $(BACKUP_DIR)/public_schema_*.sql.gz 2>/dev/null | head -1); \
	if [ -z "$$schema_file" ]; then \
		echo "ERROR: no public_schema_*.sql.gz found in $(BACKUP_DIR)"; exit 1; \
	fi; \
	echo "Restoring schema grants from $$schema_file"; \
	gzip -dc "$$schema_file" | docker exec -i $(DB_CONTAINER) psql -U $(DB_USER) $(DB_NAME)
	@for table in $(DB_TABLES); do \
		name=$$(echo $$table | tr '.' '_'); \
		f=$$(ls -t $(BACKUP_DIR)/$${name}_*.sql.gz $(BACKUP_DIR)/$${name}_*.sql.gz.part-aa 2>/dev/null | head -1); \
		if [ -z "$$f" ]; then echo "  WARNING: no backup found for $$table — skipping"; continue; fi; \
		case "$$f" in \
		*.part-aa) base=$${f%.part-aa}; \
			echo "  $$table ← $$base.part-*"; \
			cat "$$base".part-* | gzip -dc | docker exec -i $(DB_CONTAINER) psql -U $(DB_USER) $(DB_NAME);; \
		*) echo "  $$table ← $$f"; \
			gzip -dc "$$f" | docker exec -i $(DB_CONTAINER) psql -U $(DB_USER) $(DB_NAME);; \
		esac; \
	done
	@echo "Per-table restore complete from $(BACKUP_DIR)"

# ---------------------------------------------------------------------------
# Database migrations
# ---------------------------------------------------------------------------
db-migrate: ## Apply all pending Alembic migrations
	uv run alembic upgrade head

db-rollback: ## Rollback the last Alembic migration
	uv run alembic downgrade -1

db-migrate-liquibase: ## Apply Liquibase changesets against local postgres (requires compose postgres running)
	docker compose run --rm liquibase

# Number of Liquibase changesets db-rollback-liquibase reverses. One Alembic
# revision is usually represented by SEVERAL changesets, so the default of 1
# does NOT undo a whole revision. Name the changelog and let make count it:
#
#   make db-rollback-liquibase VERSION=1.7
#
# Do not hardcode a count here. A hardcoded number silently rots as a changelog
# grows, and the failure is quiet: rollbackCount reverses the last N changesets
# in EXECUTION order, so too small an N reverses the tail of the changelog —
# which for repair-style changesets is typically a run of empty <rollback/>
# no-ops — reporting success while undoing nothing.
#
# COUNT=N still overrides, for deliberately reversing part of a changelog.
VERSION ?=
CHANGELOG_FILE = changelog/db.changelog-$(VERSION).xml
COUNT ?= $(if $(VERSION),$(shell grep -c '<changeSet ' $(CHANGELOG_FILE) 2>/dev/null),1)

db-rollback-liquibase: ## Rollback Liquibase changesets: VERSION=1.7 reverses that whole changelog (or COUNT=N for an exact number, default 1)
	@if [ -n "$(VERSION)" ] && [ ! -f "$(CHANGELOG_FILE)" ]; then \
		echo "No such changelog: $(CHANGELOG_FILE)" >&2; \
		echo "Available:" >&2; ls changelog/db.changelog-*.xml >&2; \
		exit 1; \
	fi
	@if [ -z "$(COUNT)" ] || [ "$(COUNT)" -lt 1 ] 2>/dev/null; then \
		echo "Refusing to roll back $(COUNT) changeset(s)" >&2; exit 1; \
	fi
	@echo "Rolling back $(COUNT) changeset(s)$(if $(VERSION), — all of $(CHANGELOG_FILE),)"
	docker compose run --rm liquibase \
		--url=jdbc:postgresql://postgres:5432/nrf_impact \
		--username=postgres \
		--changelog-file=changelog/db.changelog.xml \
		--defaultSchemaName=public \
		rollbackCount $(COUNT)

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
load-data: ## Load all reference data into PostGIS (destructive)
	uv run python scripts/load_data.py

load-data-sample: ## Load sample data only (100 features per layer)
	uv run python scripts/load_data.py --sample

load-data-layer: ## Load a specific layer e.g. make load-data-layer LAYER=wwtw_catchments
	uv run python scripts/load_data.py --layer $(LAYER)

load-data-lookup: ## Load a specific lookup e.g. make load-data-lookup LOOKUP=wwtw_lookup
	uv run python scripts/load_data.py --lookup $(LOOKUP)

extract-fixtures: ## Clip reference layers to test input extents → tests/data/fixtures/ (requires .env.local)
	uv run python scripts/extract_test_fixtures.py

load-fixtures: ## Load committed fixture data into nrf_impact DB (no .env.local required)
	uv run python scripts/load_data.py --fixtures-dir tests/data/fixtures/

fixture-manifest: ## Regenerate checksums after updating committed fixture data
	uv run python scripts/fixture_manifest.py

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
	docker compose --profile service down --remove-orphans
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

data-sync-trigger: ## Trigger a reference-data reload: make data-sync-trigger TOKEN=xxx MANIFEST=manifest.json [FORCE=true]
	@curl -s -X POST "$(BASE_URL)/admin/data-sync?force=$(or $(FORCE),false)" \
		-H "X-Data-Sync-Token: $(TOKEN)" \
		-H "Content-Type: application/json" \
		--data @$(MANIFEST) | tee /dev/stderr

# ---------------------------------------------------------------------------
# LocalStack SNS / SQS (host gateway is default 4566 in compose.yml)
# ---------------------------------------------------------------------------
LOCALSTACK_URL ?= http://localhost:4566
AWS_LOCAL       = aws --endpoint-url=$(LOCALSTACK_URL)
# Dummy credentials LocalStack accepts. Target-specific so they stay scoped to
# these targets, and exported rather than inline-prefixed for cmd.exe's sake.
LOCALSTACK_TARGETS = sns-publish sns-publish-real sqs-send sqs-peek sqs-depth sqs-purge
$(LOCALSTACK_TARGETS): export AWS_ACCESS_KEY_ID := test
$(LOCALSTACK_TARGETS): export AWS_SECRET_ACCESS_KEY := test
$(LOCALSTACK_TARGETS): export AWS_DEFAULT_REGION := eu-west-2
SNS_TOPIC_ARN   = arn:aws:sns:eu-west-2:000000000000:nrf-quote-estimate-request
SQS_QUEUE_URL   = http://localhost:4566/000000000000/nrf-impact-assessment-jobs
SAMPLE_PAYLOAD  = scripts/sample_quote_payload.json

sns-publish: ## Publish sample quote payload to SNS (wrapped → SQS). Override: PAYLOAD=path/to.json
	$(AWS_LOCAL) sns publish \
		--topic-arn $(SNS_TOPIC_ARN) \
		--message file://$(or $(PAYLOAD),$(SAMPLE_PAYLOAD))

BACKEND_URL ?= http://localhost:3001

sns-publish-real: ## POST a quote to nrf-backend, then publish to SNS using the real reference
	@ref=$$(curl -s -X POST $(BACKEND_URL)/quotes \
		-H "Content-Type: application/json" \
		-d '{"boundaryEntryType":"draw","developmentTypes":["housing"],"housingUnits":25,"email":"developer@example.com"}' \
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
