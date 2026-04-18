-- Exécuté au premier démarrage de Postgres (volume vide).
-- Crée la DB interne utilisée par Metabase pour son stockage applicatif
-- (settings, dashboards, users, etc.), séparée de panorama_idf où on
-- exporte les marts dbt.
SELECT 'CREATE DATABASE metabase_app OWNER metabase'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase_app')\gexec
