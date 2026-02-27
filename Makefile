.PHONY: help test lint format build up down logs rebuild health monitoring-up monitoring-down monitoring-logs

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Development
# ---------------------------------------------------------------------------
test: ## Run unit tests
	uv run pytest tests/ app/ -v

lint: ## Run linter
	uv run ruff check .

format: ## Format and fix lint
	uv run ruff format . && uv run ruff check . --fix

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