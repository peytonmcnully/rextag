#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

echo "=== rextag pipeline ==="

# Step 1: Extract geodatabases to BigQuery staging
echo ""
echo "--- Extract ---"
rextag extract --config config.yml

# Step 2: Run dbt transformations
echo ""
echo "--- Transform ---"
cd dbt_project
dbt deps
dbt run

# Step 3: Run dbt tests
echo ""
echo "--- Test ---"
dbt test

echo ""
echo "=== Pipeline complete ==="
