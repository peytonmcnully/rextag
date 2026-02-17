# rextag

Geodatabase to BigQuery ELT pipeline.

## Project Structure

- `rextag/` — Python extraction package (Fiona/GDAL, GCS, BigQuery)
- `dbt_project/` — dbt transformation project (dbt-bigquery)
- `tests/` — Python tests (pytest)
- `scripts/` — Production pipeline scripts
- `config.yml` — Pipeline configuration (not committed, see config.example.yml)

## Commands

- `make install` — Install dependencies with uv
- `make test-python` — Run Python tests
- `make extract` — Run extraction pipeline
- `make transform` — Run dbt transformations
- `make pipeline` — Run full pipeline (extract + transform + test)
- `make lint` — Lint Python and compile dbt

## Conventions

- Python 3.11+, managed by uv
- Conventional commits (feat:, fix:, chore:, docs:, refactor:, test:)
- Default branch: main
- Tests: pytest for Python, dbt test for data quality
- Geometry stored as STRING in staging, cast to GEOGRAPHY in dbt models

## Architecture

GCS (.gdb.zip) → Python extraction → GeoJSONL → BigQuery staging → dbt transform → BigQuery marts

Key modules:
- `rextag/config.py` — YAML config loading
- `rextag/extract.py` — GCS download, unzip, Fiona layer reading
- `rextag/convert.py` — GeoJSONL conversion with CRS reprojection
- `rextag/schema.py` — Fiona → BigQuery type mapping
- `rextag/load.py` — GCS upload + BigQuery load jobs
- `rextag/cli.py` — Click CLI entry point
