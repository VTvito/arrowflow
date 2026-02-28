.PHONY: help up down build logs test lint benchmark clean quickstart demo-data

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Quick Start ──
quickstart: ## One-command setup: env → build → start → load demo data
	@echo "══════════════════════════════════════════════════════"
	@echo "  ETL Microservices — Quick Start"
	@echo "══════════════════════════════════════════════════════"
	@test -f .env || (cp .env.example .env && echo "✓ Created .env from .env.example")
	@test -f .env && echo "✓ .env already exists"
	docker compose build
	docker compose up -d
	@echo "⏳ Waiting for services to be healthy..."
	@sleep 10
	@echo "📦 Loading demo datasets into shared volume..."
	@$(MAKE) demo-data
	@echo ""
	@echo "══════════════════════════════════════════════════════"
	@echo "  ✓ All services running!"
	@echo ""
	@echo "  Streamlit UI:  http://localhost:8501"
	@echo "  Airflow:       http://localhost:8080"
	@echo "  Prometheus:    http://localhost:9090"
	@echo "  Grafana:       http://localhost:3000"
	@echo ""
	@echo "  Demo datasets loaded at /app/data/ in containers."
	@echo "  Try the 'ecommerce_pipeline' or"
	@echo "  'hr_analytics_pipeline' DAG in Airflow!"
	@echo "══════════════════════════════════════════════════════"

demo-data: ## Copy demo datasets into the shared Docker volume
	@mkdir -p data/demo
	@echo "Copying demo datasets to running containers..."
	@for svc in extract-csv-service clean-nan-service delete-columns-service \
		data-quality-service outlier-detection-service load-data-service; do \
		if docker ps --format '{{.Names}}' | grep -q "^$$svc$$"; then \
			docker cp data/demo/hr_sample.csv $$svc:/app/data/hr_demo/data.csv && \
			docker cp data/demo/ecommerce_orders.csv $$svc:/app/data/ecommerce_demo/data.csv && \
			echo "  ✓ $$svc"; \
		else \
			echo "  ✗ $$svc is not running — start services first with 'make up'"; \
			exit 1; \
		fi; \
	done
	@echo "✓ Demo data loaded: hr_demo/data.csv, ecommerce_demo/data.csv"

# ── Docker ──
up: ## Start all services (detached)
	docker compose up -d

down: ## Stop all services
	docker compose down

build: ## Build all Docker images
	docker compose build

rebuild: ## Rebuild all images (no cache)
	docker compose build --no-cache

logs: ## Tail logs for all services
	docker compose logs -f

logs-service: ## Tail logs for a specific service (usage: make logs-service SVC=clean-nan-service)
	docker compose logs -f $(SVC)

ps: ## Show running containers
	docker compose ps

# ── Testing ──
test: ## Run all tests
	python -m pytest tests/ -v

test-unit: ## Run unit tests only
	python -m pytest tests/unit/ -v

test-integration: ## Run integration tests only
	python -m pytest tests/integration/ -v

test-coverage: ## Run tests with coverage report
	python -m pytest tests/ --cov=services --cov-report=html --cov-report=term

# ── Code Quality ──
lint: ## Run ruff linter
	python -m ruff check .

lint-fix: ## Auto-fix lint issues
	python -m ruff check . --fix

format: ## Format code with ruff
	python -m ruff format .

# ── Benchmark ──
benchmark-data: ## Generate benchmark datasets (all scales)
	python benchmark/generate_hr_dataset.py --all-scales

benchmark-mono: ## Run monolith benchmark
	python benchmark/run_benchmark.py --mode monolith --plot

benchmark-micro: ## Run microservices benchmark (services must be running)
	python benchmark/run_benchmark.py --mode microservices --plot

benchmark-all: ## Run full comparison benchmark
	python benchmark/run_benchmark.py --mode both --plot

# ── Streamlit ──
streamlit: ## Run Streamlit app locally
	streamlit run streamlit_app/app.py

# ── Cleanup ──
clean: ## Remove generated files
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf benchmark/results/*.png benchmark/results/*.html benchmark/results/*.json
	rm -rf benchmark/data/*.csv
	rm -rf htmlcov/ .coverage .pytest_cache/
