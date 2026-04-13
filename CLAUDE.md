# CLAUDE.md

Guide pour les agents travaillant sur ce projet. Lis ce fichier en priorité avant toute modification.

## Vue d'ensemble

**Panorama Ile-de-France** est un projet d'analyse du cadre de vie en Île-de-France à partir d'open data publics. Le livrable final est un dashboard Metabase « Panorama Ile-de-France » qui croise immobilier, revenus, démographie, sécurité, éducation et mobilité.

Stack : Python (uv) + DuckDB (warehouse local) + dbt (transformations) + PostgreSQL + Metabase (visualisation) + Docker.

## Architecture en 4 couches

```
  APIs open data      →  src/panorama_idf/ingest/    (Python)
         ↓                          ↓
  Fichiers bruts           data/raw/*.csv, *.xlsx, *.zip
         ↓                          ↓
  DuckDB (raw_*)        dbt/models/staging/stg_*          (views)
         ↓                          ↓
  DuckDB (stg_*/int_*)  dbt/models/intermediate/int_*     (views)
         ↓                          ↓
  DuckDB (mart_*)       dbt/models/marts/mart_*           (tables)
         ↓                          ↓
  PostgreSQL            scripts/setup_metabase.py         (export + dashboard)
         ↓                          ↓
  Metabase dashboard    http://localhost:3000/dashboard/6
```

**Séparation stricte** : l'ingestion Python (`src/ingest`) et les transformations dbt (`dbt/`) sont indépendantes. Elles communiquent via les tables `raw_*` dans DuckDB.

## Structure du projet

```
panorama_idf/
├── CLAUDE.md                    ← ce fichier
├── README.md                    ← doc utilisateur (méthodologie, KPIs)
├── pyproject.toml               ← deps Python (uv)
├── docker-compose.yml           ← PostgreSQL + Metabase + nginx GeoJSON
│
├── data/
│   ├── raw/                     ← fichiers téléchargés par l'ingestion
│   ├── panorama_idf.duckdb ← warehouse DuckDB (512MB)
│   └── metabase/                ← GeoJSON servis par nginx pour les cartes
│
├── src/panorama_idf/ingest/
│   ├── config.py                ← liste des datasets (URL, filename, extract)
│   ├── download.py              ← HTTP download + unzip/gunzip
│   ├── prepare.py               ← chargement CSV/Excel → DuckDB raw_*
│   └── cli.py                   ← entry point `uv run ingest`
│
├── dbt/
│   ├── dbt_project.yml          ← config dbt + vars (seuils filtrage)
│   ├── profiles.yml             ← connexion DuckDB (path relatif ../data/)
│   ├── models/
│   │   ├── staging/             ← stg_*: nettoyage, typage
│   │   ├── intermediate/        ← int_*: jointures thématiques
│   │   └── marts/               ← mart_*: tables analytiques finales
│   ├── tests/                   ← tests singuliers SQL (bounds checks)
│   ├── macros/                  ← cast_filosofi_numeric, safe_divide
│   ├── seeds/zones_idf.csv      ← mapping département → zone IDF
│   └── docs/overview.md         ← page d'accueil dbt docs
│
└── scripts/
    └── setup_metabase.py        ← export mart → PG + création dashboard
```

## Commandes clés

### Ingestion (Python)
```bash
uv run ingest              # télécharge + charge tous les datasets v1 dans DuckDB
uv run ingest --force      # re-télécharge même si les fichiers existent
uv run ingest --v2         # inclut les datasets IRIS (v2)
```

### dbt (doit être lancé depuis le dossier `dbt/`)
```bash
cd dbt
uv run dbt seed --profiles-dir .     # charge seeds/zones_idf.csv
uv run dbt run --profiles-dir .      # exécute tous les modèles
uv run dbt test --profiles-dir .     # 46 tests (relationships, bounds, not_null...)
uv run dbt build --profiles-dir .    # run + test
uv run dbt docs generate --profiles-dir .   # regénère la doc
uv run dbt docs serve --profiles-dir .      # doc interactive sur :8080
```

**ATTENTION** : `--profiles-dir .` suppose que tu es dans `dbt/`. Depuis la racine, utilise `--project-dir dbt --profiles-dir dbt`.

### Dashboard Metabase
```bash
uv run python scripts/setup_metabase.py
```
Ce script fait TOUT en 11 étapes : docker up, export marts → PG, download Vélib/métro/diplômes/cyclable, setup Metabase, création/update dashboard. Le dashboard est idempotent : il est mis à jour en place (ID stable = 6 en local), pas recréé.

## Modifier le dashboard Metabase

Le script `scripts/setup_metabase.py` est **la source de vérité du dashboard**. Ne jamais modifier le dashboard via l'UI Metabase — tout changement doit passer par le script, puis être redéployé.

### Structure du script

1. **Constantes en haut** : `MART_TABLES`, `LATEST_YEAR = 2025`, `PG_CONN`, `CLR_ALT`
2. **Fonctions d'export PG** :
   - `export_marts_to_postgres()` : marts dbt + tables ad-hoc (ex: `prix_paris_par_pieces` via SQL inline)
   - `export_velib_to_postgres()` : API Vélib + jointure spatiale DuckDB
   - `export_cycling_to_postgres()` : pistes cyclables + compteurs Paris OpenData
   - `export_metro_to_postgres()` : IDFM métro+RER avec dédup par nom
   - `export_diplomes_to_postgres()` : INSEE RP 2021 diplômes
3. **Helpers viz** : `map_viz()` (choroplèthe), `pin_viz()` (points), `_heading()` (titre section)
4. **`create_tabbed_dashboard()`** : cherche le dashboard existant par nom, le met à jour en place. Si absent, le crée. Supprime tout dashboard avec un autre nom.

### Ajouter un nouveau graphique

1. Si les données ne sont pas dans PG :
   - Ajouter une entrée dans `MART_TABLES` (tuple `(SQL_source, table_name)`) **ou**
   - Créer une fonction `export_X_to_postgres()` pour les sources externes (API, spatial)
   - Appeler la fonction dans `main()` (numérotation des étapes `[X/N]`)
2. Créer la carte Metabase via `make_card()` dans `create_tabbed_dashboard()` :
   ```python
   c_name = make_card(client, db_id, "Titre", "map|bar|line|table|scatter",
       "SELECT ...", "description optionnelle",
       viz=map_viz("region_key", "metric_column"))
   ```
3. Ajouter la carte dans la liste `dashcards` du PUT final avec `_card(card_id, tab, row, col, size_x, size_y)`

### Conventions de layout

- Grille 24 colonnes
- Cartes (maps) : `size_y=10`, généralement 12 cols (paire) ou 24 cols (seule)
- Bar/line charts : `size_y=8`, généralement 12 ou 24 cols
- Tables : `size_y=7-8`, 24 cols
- Scatter : `size_y=10`, 24 cols
- Headings : `size_y=2`, 24 cols, créés via `_head(tab, row, "Titre")`

### Couleurs des cartes

Règle : **bleu Metabase par défaut partout**, sauf pour la carte de droite d'une paire côte à côte où on utilise `CLR_ALT` (vert léger 5 niveaux). Pas plus d'une couleur alternative par onglet visuellement — garder sobre.

### Ajouter/modifier une section avec titre

```python
_head(T2, 87, "Mobilité"),                    # titre à row 87
_card(c_paris_map_metro,  T2, 89,  0, 12, 10), # carte row 89 (laisse 2 rows pour heading h=2)
_card(c_paris_map_velib,  T2, 89, 12, 12, 10),
```

### Sharing public (mise en prod)

Metabase supporte le partage public sans login. Pour l'activer, il faut :
1. Settings → Public Sharing → Enable (à faire via l'UI ou via `/api/setting/enable-public-sharing`)
2. Pour chaque dashboard : Sharing → Public Link

## Modifier les modèles dbt

### Workflow

1. Modifier un `.sql` ou `.yml` dans `dbt/models/`
2. `cd dbt && uv run dbt run --profiles-dir .`  (ou `build` pour run + test)
3. Si nouvelles colonnes utilisées dans Metabase : relancer `uv run python scripts/setup_metabase.py`

### Conventions de nommage

- **Staging** : `stg_{source}__{entity}` (ex: `stg_dvf__mutations_idf`)
- **Intermediate** : `int_{domain}__{entity}` (ex: `int_immo__prix_commune_annee`)
- **Marts** : `mart_{domain}__{report}` (ex: `mart_immo__accessibilite_commune`)

Double underscore entre source/domain et entity.

### Matérialisation

- Staging + intermediate = `view` (refresh gratuit)
- Marts = `table` (persisté, plus rapide à requêter)
- Exception : `stg_dvf__mutations_idf` est `incremental` (volumétrie importante)

### Variables (seuils de filtrage)

Configurables dans `dbt_project.yml` (section `vars:`) :
```yaml
vars:
  dvf_prix_min: 1000
  dvf_prix_max: 50000000
  dvf_surface_min: 5
  dvf_surface_max: 5000
  dvf_annee_min: 2018
  communes_nb_ventes_min: 5
```

Usage dans SQL : `{{ var('dvf_prix_min') }}`. **Ne jamais hardcoder ces valeurs** dans les modèles.

### Ajouter une source de données

1. Ajouter une `DatasetConfig` dans `src/panorama_idf/ingest/config.py` avec name, url, filename, description, extract (optionnel)
2. Ajouter la fonction de chargement dans `src/panorama_idf/ingest/prepare.py`
3. Déclarer la source dans `dbt/models/staging/{domain}/_{domain}__sources.yml` avec `loaded_at_field` (sinon freshness cassé)
4. Créer `stg_{domain}__{entity}.sql` qui nettoie/type la source
5. Documenter dans `dbt/models/staging/{domain}/_{domain}__models.yml`
6. Ajouter au moins un test `not_null` sur la clé primaire

### Tests dbt

Stratégie :
- **Staging** : `not_null` + `unique` sur les clés, `accepted_values` si domaine fermé
- **Intermediate** : `relationships` vers `int_geo__communes_idf` pour tout `code_commune`
- **Marts** : `not_null` sur les colonnes analytiques critiques (prix, ventes)
- **Singular tests** dans `dbt/tests/*.sql` : bounds checks qui renvoient les rows en erreur (échec si > 0 rows)

Les tests sont exécutés avec `dbt test` ou `dbt build`.

## Sources de données (situation actuelle)

| Source | Fournisseur | Grain | Endpoint |
|--------|-------------|-------|----------|
| DVF+ | Cerema | mutation | `files.data.gouv.fr/geo-dvf/...` |
| Stats DVF | data.gouv.fr | commune | API |
| Filosofi | INSEE | commune | `insee.fr/fichier/7756855/...` |
| Population historique | INSEE | commune × année | `insee.fr/fichier/3698339/...` |
| Population par âge | INSEE RP | commune × âge | `insee.fr/fichier/8202264/...` |
| COG | INSEE | commune | `insee.fr/fichier/7766585/...` |
| Loyers | ANIL | commune | data.gouv.fr |
| Délinquance | SSMSI | commune × année | data.gouv.fr |
| Diplômes | INSEE RP | commune | `insee.fr/fichier/8202319/...` (dans setup_metabase.py) |
| Vélib | Smovengo GBFS | station | `velib-metropole-opendata.smovengo.cloud` |
| Pistes cyclables | Paris OpenData | segment | `opendata.paris.fr/api/...` |
| Métro/RER | IDFM | arrêt × ligne | `data.iledefrance-mobilites.fr/api/...` |

Les 4 dernières sources sont téléchargées **directement dans `setup_metabase.py`** (pas via l'ingestion Python ni dbt), car elles sont utilisées uniquement pour le dashboard.

## Gotchas et points d'attention

1. **Chemins relatifs dbt** : `profiles.yml` a `path: "../data/panorama_idf.duckdb"`. Toujours lancer dbt depuis `dbt/` ou utiliser `--project-dir dbt --profiles-dir dbt` depuis la racine.

2. **Ne pas hardcoder l'année** : `LATEST_YEAR = 2025` en haut de `setup_metabase.py`, utiliser `Y = str(LATEST_YEAR)` et `{Y}` dans les f-strings SQL.

3. **Catalogue DuckDB dans les vues** : les vues dbt référencent `panorama_idf.main.raw_X` avec le catalog complet. Ouvrir la base directement (pas de copie tmp) pour que les vues fonctionnent.

4. **Export inline SQL dans `MART_TABLES`** : supporte les tuples `(source, short_name)` où source peut être une query `SELECT ...` au lieu d'un nom de table.

5. **DuckDB spatial** : utilisé pour les jointures point-en-polygone (Vélib, compteurs vélo, métro). `ST_Area(geom) * 111.12 * 111.12 * 0.6583` donne les km² approximatifs à la latitude de Paris.

6. **Setup idempotent** : le script peut être relancé à volonté. Le dashboard est mis à jour en place (recherche par nom "Panorama Ile-de-France"), les autres dashboards sont supprimés automatiquement.

7. **Metabase setup-token** : après réinit de Metabase, le setup-token peut persister. `setup_admin()` gère le fallback 403 → login.

8. **Accents dans les titres** : utiliser les vrais accents (é, è, ç) dans les titres de cartes et sections. Les queries SQL peuvent garder l'ASCII pour compat.

## Tests et qualité

État actuel après cleanup :
- **46 tests** (vs 31 avant cleanup)
- Tous les `select *` remplacés par colonnes explicites
- Variables externalisées (plus de seuils hardcodés dans les modèles)
- `docs/overview.md` existe pour dbt docs

Commande de référence : `cd dbt && uv run dbt build --profiles-dir .` doit passer à 0 erreur.

## Déploiement

Pour déployer ailleurs (VPS, serveur) :
1. Clone du repo
2. `uv sync`
3. `docker compose up -d`
4. `uv run ingest` (téléchargement initial, ~10 min)
5. `cd dbt && uv run dbt build --profiles-dir .`
6. `uv run python scripts/setup_metabase.py`

Le dashboard est accessible sur le port 3000. Pour HTTPS, ajouter un reverse proxy (Caddy recommandé pour auto Let's Encrypt).

## Instructions pour l'agent

- **Toujours lire `CLAUDE.md`, `README.md` et `dbt/docs/overview.md`** avant modifications
- **Ne jamais modifier le dashboard Metabase via l'UI** — tout passe par `setup_metabase.py`
- **Conserver les conventions de nommage** dbt (stg_, int_, mart_)
- **Ajouter un test** pour toute nouvelle colonne critique (not_null au minimum)
- **Utiliser `{{ var('X') }}`** pour tout seuil de filtrage
- **Vérifier avec `dbt build`** que rien n'est cassé avant de considérer une tâche terminée
- **Commits en français**, style descriptif du « pourquoi » plutôt que du « quoi »
