PYTHON ?= .venv/bin/python
PIP ?= .venv/bin/pip
export PYTHONPATH := src
export SPARK_LOCAL_IP ?= 127.0.0.1

.PHONY: setup generate generate-tiny generate-medium pipeline test lint format dashboard diagrams clean

setup:
	python3 -m venv .venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements-dev.txt

generate:
	$(PYTHON) -m northline.cli generate --scale small --output data/sample --seed 42

generate-tiny:
	$(PYTHON) -m northline.cli generate --scale tiny --output data/landing/tiny --seed 42

generate-medium:
	$(PYTHON) -m northline.cli generate --scale medium --output data/landing/medium --seed 42

pipeline:
	$(PYTHON) -m northline.cli run --env dev

test:
	$(PYTHON) -m pytest tests --cov=northline --cov-report=term-missing

lint:
	$(PYTHON) -m ruff check src tests dashboard
	$(PYTHON) -m ruff format --check src tests dashboard

format:
	$(PYTHON) -m ruff format src tests dashboard
	$(PYTHON) -m ruff check --fix src tests dashboard

dashboard:
	$(PYTHON) -m northline.cli dashboard --port 8501

diagrams:
	$(PYTHON) scripts/render_diagrams.py

clean:
	rm -rf data/lake data/lake-test data/logs data/logs-test data/landing .pytest_cache .ruff_cache htmlcov .coverage coverage.xml
