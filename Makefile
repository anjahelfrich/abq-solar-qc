.PHONY: setup test lint format dagster backfill snapshot app

export DAGSTER_HOME := $(CURDIR)/.dagster

START ?= 2026-08-01
END ?= 2026-10-31

setup:
	uv sync

test:
	uv run pytest

lint:
	uv run ruff check .
	uv run ruff format --check .

format:
	uv run ruff check --fix .
	uv run ruff format .

dagster:
	uv run dagster dev -m abq_qc.definitions

# Stop `make dagster` first: DuckDB allows only one process to write at a time.
backfill:
	uv run python -m abq_qc.backfill $(START) $(END)

snapshot:
	uv run dagster asset materialize -m abq_qc.definitions --select snapshot

# Reads the committed snapshot/ only. Run `make snapshot` first to show new data.
app:
	uv run streamlit run dashboard/app.py
