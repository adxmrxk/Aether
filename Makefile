.PHONY: help install dev test lint format deploy clean docker-up docker-down

# Default target
help:
	@echo "AetherFlow - Development Commands"
	@echo ""
	@echo "Setup:"
	@echo "  make install      Install all dependencies"
	@echo "  make dev          Start local development environment"
	@echo ""
	@echo "Testing:"
	@echo "  make test         Run all tests"
	@echo "  make test-cov     Run tests with coverage"
	@echo "  make lint         Run linters"
	@echo "  make format       Format code"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make pulumi-up    Deploy with Pulumi"
	@echo "  make tf-plan      Terraform plan"
	@echo "  make tf-apply     Terraform apply"
	@echo ""
	@echo "Docker:"
	@echo "  make docker-up    Start all services"
	@echo "  make docker-down  Stop all services"
	@echo ""
	@echo "Data:"
	@echo "  make dbt-run      Run dbt models"
	@echo "  make dbt-test     Run dbt tests"

# ============================================================================
# Setup
# ============================================================================

install:
	pip install -r api/requirements.txt
	pip install -r functions/market_processor/requirements.txt
	pip install -r tests/requirements.txt
	pip install -r observability/requirements.txt
	cd dbt && pip install dbt-core dbt-bigquery && dbt deps

dev: docker-up
	cd api && uvicorn main:app --reload --host 0.0.0.0 --port 8080

# ============================================================================
# Testing
# ============================================================================

test:
	pytest tests/ -v

test-cov:
	pytest tests/ -v --cov=api --cov=functions --cov-report=html

test-integration:
	pytest tests/integration/ -v

lint:
	ruff check .
	mypy api/ --ignore-missing-imports

format:
	ruff format .
	ruff check --fix .

# ============================================================================
# Infrastructure - Pulumi
# ============================================================================

pulumi-up:
	cd infra && pulumi up

pulumi-preview:
	cd infra && pulumi preview

pulumi-destroy:
	cd infra && pulumi destroy

# ============================================================================
# Infrastructure - Terraform
# ============================================================================

tf-init:
	cd terraform && terraform init

tf-plan:
	cd terraform && terraform plan

tf-apply:
	cd terraform && terraform apply

tf-destroy:
	cd terraform && terraform destroy

# ============================================================================
# Docker
# ============================================================================

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f

docker-build:
	docker-compose build --no-cache

# ============================================================================
# dbt
# ============================================================================

dbt-run:
	cd dbt && dbt run

dbt-test:
	cd dbt && dbt test

dbt-docs:
	cd dbt && dbt docs generate && dbt docs serve

dbt-fresh:
	cd dbt && dbt run --full-refresh

# ============================================================================
# Deployment
# ============================================================================

deploy-function:
	gcloud functions deploy aether-market-processor \
		--gen2 \
		--runtime=python312 \
		--region=us-central1 \
		--source=functions/market_processor \
		--entry-point=process_market_data \
		--trigger-topic=aether-market-data \
		--memory=512MB \
		--timeout=300s

deploy-api:
	cd api && gcloud run deploy aether-api \
		--source=. \
		--region=us-central1 \
		--allow-unauthenticated

# ============================================================================
# Cleanup
# ============================================================================

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +
	find . -type d -name "htmlcov" -exec rm -rf {} +
	cd dbt && rm -rf target/ dbt_packages/ logs/
