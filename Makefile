.PHONY: install extract transform test pipeline lint clean

install:
	uv sync

extract:
	uv run rextag extract --config config.yml

transform:
	cd dbt_project && dbt run

test-dbt:
	cd dbt_project && dbt test

test-python:
	uv run pytest tests/ -v

test: test-python test-dbt

pipeline: extract transform test-dbt

lint:
	uv run ruff check rextag/ tests/
	cd dbt_project && dbt compile

clean:
	rm -rf dbt_project/target dbt_project/dbt_packages dbt_project/logs
