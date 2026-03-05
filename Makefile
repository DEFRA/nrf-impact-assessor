.PHONY: help test lint format build up down logs rebuild health monitoring-up monitoring-down monitoring-logs load-data load-data-sample load-data-layer load-data-lookup db-migrate db-rollback

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
# Database migrations
# ---------------------------------------------------------------------------
DB_MIGRATE_ENV = DB_IAM_AUTHENTICATION=false DB_HOST=localhost

db-migrate: ## Apply all pending Alembic migrations
	$(DB_MIGRATE_ENV) uv run alembic upgrade head

db-rollback: ## Rollback the last Alembic migration
	$(DB_MIGRATE_ENV) uv run alembic downgrade -1

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
LOAD_DATA_ENV = PYTHONPATH=. DB_IAM_AUTHENTICATION=false DB_HOST=localhost

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
# Monitoring
# ---------------------------------------------------------------------------
MONITORING_COMPOSE = docker compose -f compose.yml -f compose.monitoring.yml

monitoring-up: ## Start all services including monitoring stack
	$(MONITORING_COMPOSE) --profile service up -d

monitoring-down: ## Stop all services including monitoring stack
	$(MONITORING_COMPOSE) down

monitoring-logs: ## Tail monitoring stack logs
	$(MONITORING_COMPOSE) logs -f timescaledb vector grafana

# ---------------------------------------------------------------------------
# Test endpoints
# ---------------------------------------------------------------------------
BASE_URL ?= http://localhost:8085

health: ## Check health endpoint
	curl -s $(BASE_URL)/health | python -m json.tool