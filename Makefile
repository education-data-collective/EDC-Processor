.PHONY: help install install-dev test test-cov lint format clean run build docker-build

# Default target
help:
	@echo "Available commands:"
	@echo "  install      Install production dependencies"
	@echo "  install-dev  Install development dependencies"
	@echo "  test         Run all tests"
	@echo "  test-unit    Run unit tests only (fast, mocked)"
	@echo "  test-integration Run integration tests (slower, real services)"
	@echo "  test-integration-db Run database integration tests"
	@echo "  test-integration-firebase Run Firebase integration tests"
	@echo "  test-fast    Run only unit tests with minimal output"
	@echo "  test-cov     Run tests with coverage report"
	@echo "  lint         Run linting tools"
	@echo "  format       Format code with black and isort"
	@echo "  security     Run security checks"
	@echo "  clean        Clean up cache files"
	@echo "  run          Run the development server"
	@echo "  build        Build the package"
	@echo "  docker-build Build Docker image"

# Installation
install:
	pip install -r requirements.txt

install-dev:
	pip install -r requirements-dev.txt
	pre-commit install

# Testing
test:
	pytest

test-cov:
	pytest --cov=entity_processing --cov=models --cov-report=html --cov-report=term-missing

test-unit:
	pytest -m "unit" -v

test-integration:
	pytest -m "integration" -v --tb=short

test-integration-db:
	@echo "Running database integration tests..."
	pytest tests/integration/test_database_integration.py -v

test-integration-firebase:
	@echo "Running Firebase integration tests..."
	pytest tests/integration/test_firebase_integration.py -v

test-all:
	pytest -v

test-fast:
	pytest -m "unit" --tb=short --no-cov

# Code Quality
lint:
	flake8 entity_processing models tests
	mypy entity_processing models
	bandit -r entity_processing models

format:
	black entity_processing models tests
	isort entity_processing models tests

format-check:
	black --check entity_processing models tests
	isort --check-only entity_processing models tests

security:
	bandit -r entity_processing models
	safety check

# Cleaning
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.pyd" -delete
	find . -type f -name ".coverage" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -exec rm -rf {} +
	rm -rf build/
	rm -rf dist/
	rm -rf htmlcov/

# Development
run:
	flask run --debug

run-prod:
	gunicorn -c gunicorn.conf.py app:app

# Building
build:
	python -m build

# Docker
docker-build:
	docker build -t edc-processor .

docker-run:
	docker run -p 8080:8080 edc-processor

# Database
db-migrate:
	flask db migrate

db-upgrade:
	flask db upgrade

db-downgrade:
	flask db downgrade

# Git workflow helpers
branch-clean:
	git branch --merged main | grep -v main | xargs -n 1 git branch -d

push-dev:
	git push origin develop

push-main:
	git push origin main

# Pre-commit hook simulation
pre-commit: format-check lint test-unit
	@echo "✅ Pre-commit checks passed!" 