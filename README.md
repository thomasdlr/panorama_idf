# Panorama Île-de-France

Dashboard Metabase croisant immobilier, revenus, démographie, sécurité, éducation et mobilité en Île-de-France, à partir d'open data publics.

## Stack

Python (uv) + DuckDB + dbt + PostgreSQL + Metabase, le tout en Docker.

## Lancer en local

```bash
just setup         # uv sync
just ingest        # téléchargement (~500 MB, 10 min)
just dbt-all       # transformations + tests
just metabase-up   # dashboard sur http://localhost:3000
```

Ou en une commande : `just all`.

Admin local par défaut : `admin@panorama-idf.local` / `PanoramaIdf2024!`.

## Sources

DVF+ (Cerema), Filosofi & RP 2021 (INSEE), COG (INSEE), Loyers ANIL, Délinquance SSMSI, Vélib, pistes cyclables Paris, métro/RER IDFM.

## Architecture

```
APIs → src/panorama_idf/ingest → DuckDB (raw_*)
                              → dbt (stg_ / int_ / mart_)
                              → PostgreSQL → Metabase
```

`scripts/setup_metabase.py` est la **source de vérité du dashboard** : ne jamais l'éditer via l'UI.

## Pour aller plus loin

- [`CLAUDE.md`](./CLAUDE.md) — conventions, gotchas, workflow
- [`DEPLOYMENT.md`](./DEPLOYMENT.md) — déploiement VPS
- `dbt docs serve` — lineage et docs des modèles

## Licence

Données sous Licence Ouverte Etalab / ODbL selon les sources.
