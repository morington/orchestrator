.DEFAULT_GOAL := help
EXAMPLE ?= examples/pipeline_linear.json

.PHONY: help sync lint fmt test test-int up down stack migrate migrate-local run mocks e2e clean

help: ## Показать список целей
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

sync: ## Установить зависимости (uv)
	uv sync

lint: ## Ruff + bandit
	uv run ruff check src tests
	uv run bandit -q -r src

fmt: ## Автоформат и автофикс (ruff)
	uv run ruff format src tests mocks
	uv run ruff check --fix src tests mocks

test: ## Unit-тесты
	uv run pytest tests/units -q

test-int: ## Интеграционные тесты (нужен PostgreSQL)
	uv run pytest tests/integrations -m integration -q

up: ## Поднять postgres + nats
	docker compose up -d postgres nats

stack: ## Поднять весь стек (миграции + оркестратор)
	docker compose up -d --build

down: ## Остановить инфраструктуру
	docker compose down

migrate: ## Применить миграции через контейнер migration
	docker compose up --build migration

migrate-local: ## Применить миграции локально (POSTGRESQL__HOST=localhost в .env)
	uv run alembic upgrade head

run: ## Запустить оркестратор локально
	uv run orchestrator

mocks: ## Запустить mock-микросервисы (service.*)
	DEV=true uv run python -m mocks.service

e2e: ## Отправить пример декларации (EXAMPLE=...)
	DEV=true uv run python -m mocks.send $(EXAMPLE)

clean: ## Удалить кэши
	find . -type d -name __pycache__ -prune -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache .ruff_cache
