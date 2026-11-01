.PHONY: setup test lint format dagster

export DAGSTER_HOME := $(CURDIR)/.dagster

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
