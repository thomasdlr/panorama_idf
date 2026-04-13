#!/usr/bin/env bash
# Backup DuckDB + PostgreSQL (marts + Metabase appdb) avec rotation 7 jours.
#
# Usage :
#   ./scripts/backup.sh                 # dump dans ~/backups
#   BACKUP_DIR=/chemin ./scripts/backup.sh
#
# Cron (tous les jours a 3h du matin) :
#   0 3 * * * cd /home/thomas/panorama_idf && ./scripts/backup.sh >> logs/backup.log 2>&1
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BACKUP_DIR="${BACKUP_DIR:-$HOME/backups}"
RETENTION_DAYS="${RETENTION_DAYS:-7}"
DATE="$(date +%F)"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"

mkdir -p "$BACKUP_DIR"
cd "$PROJECT_ROOT"

# Charge .env si present (POSTGRES_USER, POSTGRES_DB)
[ -f .env ] && set -a && . ./.env && set +a

PG_USER="${POSTGRES_USER:-metabase}"
PG_DB="${POSTGRES_DB:-panorama_idf}"

echo "[$(date -Iseconds)] Backup start → $BACKUP_DIR"

# 1. DuckDB (copie fichier — DuckDB est single-writer, snapshot suffisant)
if [ -f "data/panorama_idf.duckdb" ]; then
    cp "data/panorama_idf.duckdb" "$BACKUP_DIR/duckdb-$DATE.duckdb"
    gzip -f "$BACKUP_DIR/duckdb-$DATE.duckdb"
    echo "  DuckDB   : duckdb-$DATE.duckdb.gz"
else
    echo "  DuckDB   : absent (skip)"
fi

# 2. Postgres marts (panorama_idf) — dump SQL compresse
docker compose -f "$COMPOSE_FILE" exec -T postgres \
    pg_dump -U "$PG_USER" "$PG_DB" | gzip > "$BACKUP_DIR/pg-marts-$DATE.sql.gz"
echo "  PG marts : pg-marts-$DATE.sql.gz"

# 3. Postgres Metabase appdb (questions, dashboards, users)
docker compose -f "$COMPOSE_FILE" exec -T postgres \
    pg_dump -U "$PG_USER" metabase_app | gzip > "$BACKUP_DIR/pg-metabase-$DATE.sql.gz"
echo "  PG MB    : pg-metabase-$DATE.sql.gz"

# 4. Rotation — supprime les backups de plus de RETENTION_DAYS jours
find "$BACKUP_DIR" -type f \( -name "duckdb-*.duckdb.gz" -o -name "pg-*.sql.gz" \) \
    -mtime "+$RETENTION_DAYS" -print -delete | sed 's/^/  cleanup : /'

echo "[$(date -Iseconds)] Backup done"
