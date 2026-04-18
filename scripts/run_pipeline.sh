#!/usr/bin/env bash
# Pipeline de bout en bout exécuté dans le conteneur `pipeline`.
# 1. Ingestion des données ouvertes dans DuckDB
# 2. Build dbt (seed + run + test)
# 3. Export vers Postgres + création/maj du dashboard Metabase
set -euo pipefail

cd /app

echo "═══ [1/3] Ingestion ═══"
uv run ingest "$@"

echo "═══ [2/3] dbt build ═══"
cd dbt
uv run dbt deps --profiles-dir .
uv run dbt seed --profiles-dir .
uv run dbt build --profiles-dir .
cd ..

echo "═══ [3/3] Setup Metabase ═══"
uv run python scripts/setup_metabase.py

echo "✓ Pipeline terminé"
