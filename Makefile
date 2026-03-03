.DEFAULT_GOAL := help

# ─── Configuration ───────────────────────────────────────────────────────────
COMPOSE := docker compose
API_DIR := api

# ─── Help ────────────────────────────────────────────────────────────────────
.PHONY: help
help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ─── Prerequisite Check ─────────────────────────────────────────────────────
.PHONY: check
check: ## Verify all prerequisites (Docker, Ollama, data.csv)
	@echo "Checking prerequisites..."
	@echo ""
	@command -v docker >/dev/null 2>&1 \
		&& echo "  ✔ docker            $$(docker --version | head -1)" \
		|| { echo "  ✘ docker            NOT FOUND — install Docker Desktop: https://www.docker.com/products/docker-desktop/"; exit 1; }
	@docker compose version >/dev/null 2>&1 \
		&& echo "  ✔ docker compose    $$(docker compose version --short)" \
		|| { echo "  ✘ docker compose    NOT FOUND — included with Docker Desktop"; exit 1; }
	@docker info >/dev/null 2>&1 \
		&& echo "  ✔ Docker daemon     running" \
		|| { echo "  ✘ Docker daemon     NOT RUNNING — start Docker Desktop"; exit 1; }
	@command -v ollama >/dev/null 2>&1 \
		&& echo "  ✔ ollama            $$(ollama --version 2>&1 | head -1)" \
		|| { echo "  ✘ ollama            NOT FOUND — install: https://ollama.com/download"; exit 1; }
	@curl -sf http://localhost:11434/api/tags >/dev/null 2>&1 \
		&& echo "  ✔ ollama serve      reachable at :11434" \
		|| echo "  ⚠ ollama serve      NOT RUNNING — run 'ollama serve' in another terminal (ingestion retries for up to 120s)"
	@test -f data.csv \
		&& echo "  ✔ data.csv          present ($$(wc -l < data.csv | tr -d ' ') lines)" \
		|| { echo "  ✘ data.csv          MISSING — must be in project root"; exit 1; }
	@echo ""
	@echo "All critical prerequisites met."

# ─── Start / Stop ────────────────────────────────────────────────────────────
.PHONY: start
start: check ## Run checks, then start all services (builds if needed)
	$(COMPOSE) up --build

.PHONY: stop
stop: ## Stop all services
	$(COMPOSE) down

.PHONY: restart
restart: stop start ## Stop then start all services

# ─── Logs ────────────────────────────────────────────────────────────────────
.PHONY: logs
logs: ## Tail logs from all services
	$(COMPOSE) logs -f --tail=100

.PHONY: logs-ingestion
logs-ingestion: ## Tail ingestion service logs only
	$(COMPOSE) logs -f --tail=100 ingestion

.PHONY: logs-api
logs-api: ## Tail API service logs only
	$(COMPOSE) logs -f --tail=100 api

# ─── Tests ───────────────────────────────────────────────────────────────────
.PHONY: test
test: ## Run unit tests (no Docker/Ollama needed)
	cd $(API_DIR) && PYTHONPATH=. python -m pytest tests/unit -v

.PHONY: test-integration
test-integration: ## Run integration tests (requires 'make start' first)
	cd $(API_DIR) && PYTHONPATH=. python -m pytest tests/integration -v

.PHONY: test-all
test-all: ## Run all tests (integration auto-skips if stack is down)
	cd $(API_DIR) && PYTHONPATH=. python -m pytest tests/ -v

# ─── Cleanup ─────────────────────────────────────────────────────────────────
.PHONY: clean
clean: ## Stop services and remove volumes (deletes SQLite DB; re-pulls models on next start)
	$(COMPOSE) down -v
	@echo "Volumes removed. Next 'make start' will re-ingest data and re-pull models."
