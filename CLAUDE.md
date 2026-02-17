# rextag

Geodatabase to BigQuery ELT pipeline using external tables.

## Project Structure

- `rextag/` — Python extraction package (Fiona/GDAL, GCS)
- `dbt_project/` — dbt transformation project (dbt-bigquery, dbt-external-tables)
- `tests/` — Python tests (pytest)
- `scripts/` — Production pipeline scripts
- `config.yml` — Pipeline configuration (not committed, see config.example.yml)

## Commands

- `make install` — Install dependencies with uv
- `make scan` — Scan GCS for geodatabases and generate dbt source definitions
- `make extract` — Extract geodatabases to hive-partitioned GCS paths
- `make transform` — Create external tables and run dbt transformations
- `make test-python` — Run Python tests
- `make pipeline` — Run full pipeline (extract + transform + test)
- `make lint` — Lint Python and compile dbt

## Conventions

- Python 3.11+, managed by uv
- Conventional commits (feat:, fix:, chore:, docs:, refactor:, test:)
- Default branch: main
- Tests: pytest for Python, dbt test for data quality
- External tables with json_extension=geojson for geometry layers
- Hive partitioning on data_drop for automatic partition discovery

## Architecture

```
GCS source zips (data_drop=YYYY-MM/*.zip)
  -> rextag scan     (discover schemas, generate dbt files)
  -> rextag extract  (convert to geojsonl/jsonl, upload to hive GCS paths)
  -> dbt run         (external tables read from GCS, mart transformations)
```

Key modules:
- `rextag/config.py` — YAML config loading
- `rextag/scan.py` — Schema discovery + dbt file generation
- `rextag/extract.py` — GCS download, unzip, Fiona layer reading, helpers
- `rextag/convert.py` — GeoJSONL conversion with CRS reprojection
- `rextag/schema.py` — Fiona → BigQuery type mapping
- `rextag/load.py` — GCS upload
- `rextag/cli.py` — Click CLI (scan, extract, list commands)

## GCS Layout

Source: `gs://bucket/rextagsource/data_drop=YYYY-MM/{dataset}.zip`
Staged: `gs://bucket/staged/{dataset}/{layer}/data_drop=YYYY-MM/data.{geojsonl|jsonl}`
