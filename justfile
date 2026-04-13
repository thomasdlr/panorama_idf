# Panorama Ile-de-France — Commandes principales
# Lister les commandes : just --list

# Installation des dépendances Python
setup:
    uv sync

# Téléchargement et chargement des données
ingest:
    uv run ingest

# Ingestion avec datasets V2 (IRIS)
ingest-v2:
    uv run ingest --v2

# Forcer le re-téléchargement
ingest-force:
    uv run ingest --force

# Installer les packages dbt
dbt-deps:
    cd dbt && uv run dbt deps

# Lancer les transformations
dbt-run:
    cd dbt && uv run dbt run

# Lancer les tests
dbt-test:
    cd dbt && uv run dbt test

# Générer et servir la documentation dbt
dbt-docs:
    cd dbt && uv run dbt docs generate
    cd dbt && uv run dbt docs serve --port 8080

# Tout dbt : deps + run + test
dbt-all: dbt-deps dbt-run dbt-test

# Pipeline de bout en bout : setup → ingest → dbt
all: setup ingest dbt-all

# Exporter les marts en CSV
export:
    mkdir -p data/processed
    cd dbt && uv run dbt run -s mart_immo__accessibilite_commune mart_immo__ranking_tension mart_immo__synthese_zone mart_immo__evolution_prix
    uv run python -c "\
    import duckdb; \
    con = duckdb.connect('data/panorama_idf.duckdb'); \
    tables = ['marts.mart_immo__accessibilite_commune', 'marts.mart_immo__ranking_tension', 'marts.mart_immo__synthese_zone', 'marts.mart_immo__evolution_prix']; \
    [con.execute(f\"COPY (SELECT * FROM {t}) TO 'data/processed/{t.split('.')[-1]}.csv' (HEADER, DELIMITER ',')\") or print(f'Exported {t.split(\".\")[-1]}.csv') for t in tables]; \
    con.close()"

# Lancer Metabase (Docker) et configurer le dashboard
metabase-up:
    uv run python scripts/setup_metabase.py

# Arrêter Metabase
metabase-down:
    docker compose down

# Nettoyage des artefacts dbt et du warehouse
clean:
    cd dbt && uv run dbt clean
    rm -f data/panorama_idf.duckdb data/panorama_idf.duckdb.wal
