.PHONY: up down seed demo test lint typecheck format install demo-local

install:
	pip install -e ".[dev]"

up:
	docker compose up -d --build
	docker compose ps

down:
	docker compose down -v

seed:
	python scripts/seed_data.py --rows 5000 --out data/sample_sales.csv

demo:
	etl run pipelines/sales_etl.yaml

demo-local:
	POSTGRES_URL=$${POSTGRES_URL:-postgresql://etl:etl@localhost:5432/etl} etl run pipelines/sales_etl.yaml || true

test:
	pytest -v --cov=etl --cov-fail-under=80

lint:
	ruff check src tests scripts
	ruff format --check src tests scripts

typecheck:
	mypy src

format:
	ruff format src tests scripts
	ruff check --fix src tests scripts
