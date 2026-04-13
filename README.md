# Panorama Ile-de-France

Analyse du **cadre de vie en Île-de-France** à partir d'open data publics français : immobilier, revenus, démographie, délinquance, éducation et mobilité.

Le livrable est un dashboard Metabase **Panorama Ile-de-France** avec 3 onglets (Ile-de-France, Paris, Petite couronne) et 45 cartes (choroplèthes communales, séries temporelles, classements).

## Objectif

Produire des **indicateurs territoriaux robustes** croisant plusieurs dimensions du cadre de vie, pour répondre à des questions comme :

- Où le marché immobilier est le plus tendu par rapport aux revenus locaux ?
- Comment varient délinquance, loyers et niveaux de diplôme selon les communes ?
- Quelles communes combinent bonne desserte (métro, RER, Vélib, pistes cyclables) et pression immobilière modérée ?
- Comment comparer Paris, petite couronne et grande couronne sur chacun de ces axes ?

## Ce que ce projet n'est PAS

- Il ne prédit pas les prix immobiliers.
- Il ne mesure pas l'âge des acheteurs (DVF ne contient pas cette information en open data).
- Il ne fait aucune affirmation causale (« les prix augmentent *parce que* … »).
- L'angle est : **croisement thématique de données publiques**, sans modèle.

## Limites méthodologiques

| Sujet | Limite | Impact |
|-------|--------|--------|
| **Revenus** | Filosofi mesure des revenus fiscaux et sociaux, pas les revenus réels de tous les ménages. Secret statistique sur petites communes. | Certaines communes manquent de données revenus. |
| **Prix immobiliers** | DVF/DVF+ enregistre les transactions, pas les prix du marché. Biais vers les biens effectivement vendus. | Communes avec peu de ventes = médianes instables (filtrées si < 5 ventes, seuil `communes_nb_ventes_min` dans `dbt_project.yml`). |
| **Âge** | Le recensement donne la structure d'âge des *résidents*, pas des *acheteurs*. | La part des 25-39 ans est un indicateur de composition locale, pas un proxy de l'accession. |
| **Délinquance** | SSMSI publie des taux pour 1000 habitants, sensibles à la qualité de plainte locale et aux populations de passage. | Comparer en tendance ; éviter les comparaisons absolues sur très petites communes. |
| **Loyers** | ANIL couvre principalement les communes de plus de 10 000 habitants (carte des loyers). | Couverture partielle en grande couronne. |
| **Mobilité** | Vélib ne couvre que Paris + proche banlieue ; pistes cyclables uniquement Paris ; métro/RER urbanisé. | Les densités sont non-comparables hors zone dense. |
| **Temporalité** | Filosofi et RP sont fixes (2021), seuls les prix DVF varient par année. | Les ratios prix/revenu sont des proxys, pas des taux d'effort réels. |
| **Géographie** | Le COG évolue chaque année (fusions). V1 au grain commune ; IRIS en V2. | Jointures approximatives possibles sur communes récemment fusionnées. |

## Datasets

| Source | Description | Grain | Producteur |
|--------|-------------|-------|------------|
| **DVF+ (Cerema)** | Transactions immobilières géolocalisées | Mutation | Cerema / DGFiP |
| **Statistiques DVF** | Agrégats prix / volumes prêts à l'emploi | Commune × année | data.gouv.fr |
| **Filosofi 2021** | Revenus, niveau de vie, pauvreté | Commune | INSEE |
| **RP 2021 — Population** | Population légale communale | Commune | INSEE |
| **RP 2021 — Âge** | Population par sexe et âge quinquennal | Commune × âge | INSEE |
| **RP 2021 — Diplômes** | Niveaux de diplôme | Commune | INSEE |
| **COG** | Référentiel des communes | Commune | INSEE |
| **ANIL — Loyers** | Loyers de marché au m² | Commune | ANIL / data.gouv.fr |
| **SSMSI — Délinquance** | Faits constatés par catégorie | Commune × année × catégorie | Ministère de l'Intérieur |
| **Vélib** | Stations Vélib actives | Station | Smovengo GBFS |
| **Pistes cyclables Paris** | Linéaire cyclable + compteurs | Segment | Paris OpenData |
| **Métro / RER** | Arrêts et lignes IDFM | Arrêt × ligne | Île-de-France Mobilités |

Les 4 derniers datasets (Vélib, cyclable, métro, diplômes) sont téléchargés directement par `scripts/setup_metabase.py` car ils alimentent uniquement le dashboard. Les autres passent par l'ingestion Python (`uv run ingest`).

## KPI calculés

Côté **immo** (marts dbt) :

| Indicateur | Définition | Interprétation |
|-----------|------------|----------------|
| `prix_m2_median` | Médiane du prix au m² (DVF) | Prix observé |
| `prix_median` | Médiane du prix total de transaction | Prix observé |
| `ratio_prix_m2_revenu_mensuel` | Prix m² / (niveau de vie médian / 12) | Nombre de mois de niveau de vie pour 1 m² |
| `ratio_achat_revenu_annuel` | Prix médian / niveau de vie médian | Nombre d'années de niveau de vie pour un achat médian |
| `ratio_achat_revenu_q1` | Prix médian / niveau de vie Q1 | Idem pour les ménages du 1er quartile |
| `indice_tension` | Score composite 0-100 | Classement relatif entre communes IDF |
| `part_25_39` / `part_60_plus` | Part de la tranche d'âge dans la population | Structure démographique |

Côté **cadre de vie** (calculs ad-hoc dans `setup_metabase.py`) :

| Indicateur | Source | Description |
|-----------|--------|-------------|
| `loyer_m2_median` | ANIL | Loyer de marché au m² |
| `delinquance_pour_1000` | SSMSI | Faits par catégorie pour 1 000 habitants |
| `part_sans_diplome` / `part_sup` | INSEE RP | Part population ≥ 15 ans hors scolarité |
| `densite_velib` / `densite_metro_rer` | Vélib, IDFM | Stations par km² (jointure spatiale DuckDB) |

### Indice de tension immo — composition

| Composante | Poids | Logique |
|-----------|-------|---------|
| Ratio achat/revenu annuel | 40% | Effort d'achat pur |
| Ratio m²/revenu mensuel | 30% | Densité de prix |
| Faible présence 25-39 ans | 15% | Absence de jeunes adultes |
| Taux de pauvreté | 15% | Fragilité économique |

Chaque composante est normalisée en percentile rank puis pondérée. Score final = percentile composite × 100.

## Architecture du projet

```
panorama_idf/
├── pyproject.toml              # Dépendances Python (uv)
├── justfile                    # Commandes de lancement (just)
├── README.md
├── CLAUDE.md                   # Instructions pour agents IA
├── DEPLOYMENT.md               # Guide de déploiement VPS
├── .env.example                # Template des secrets (prod)
├── docker-compose.yml          # Stack locale (dev)
├── docker-compose.prod.yml     # Stack prod (Caddy + HTTPS)
├── Caddyfile                   # Reverse proxy → Metabase
│
├── src/panorama_idf/
│   └── ingest/                 # Téléchargement + chargement DuckDB
│       ├── config.py           # URLs et métadonnées des datasets
│       ├── download.py         # HTTP download + unzip
│       ├── prepare.py          # CSV/Excel → DuckDB raw_*
│       └── cli.py              # Point d'entrée CLI
│
├── dbt/
│   ├── dbt_project.yml         # Vars de filtrage (seuils DVF)
│   ├── profiles.yml            # Connexion DuckDB
│   ├── packages.yml
│   │
│   ├── models/
│   │   ├── staging/
│   │   │   ├── dvf/            # stg_dvf__stats_communales, stg_dvf__mutations_idf
│   │   │   ├── insee/          # stg_insee__cog_communes, filosofi, population, âge
│   │   │   └── logement/       # stg_logement__loyers_communes, delinquance_communes, delinquance_detail
│   │   │
│   │   ├── intermediate/
│   │   │   ├── int_geo__communes_idf             # Référentiel communes IDF
│   │   │   ├── int_immo__prix_commune_annee      # Prix agrégés commune × année
│   │   │   ├── int_revenus__commune              # Revenus + population
│   │   │   └── int_demo__structure_age_commune   # Structure d'âge
│   │   │
│   │   └── marts/
│   │       ├── mart_immo__accessibilite_commune  # Table analytique principale
│   │       ├── mart_immo__synthese_zone          # Synthèse Paris / PC / GC
│   │       └── mart_immo__evolution_prix         # Évolution temporelle
│   │
│   ├── macros/                 # cast_filosofi_numeric, safe_divide
│   ├── tests/                  # Tests singuliers (bounds, cohérence)
│   └── seeds/                  # zones_idf.csv (dept → zone)
│
├── scripts/
│   ├── setup_metabase.py       # Export marts + config dashboard (source de vérité)
│   └── backup.sh               # Backup DuckDB + PG (rotation 7j)
│
└── data/
    ├── raw/                    # Fichiers téléchargés (non versionnés)
    ├── metabase/               # GeoJSON servis par nginx (cartes Metabase)
    └── panorama_idf.duckdb     # Warehouse local
```

## Lineage dbt

```
Sources (DuckDB raw_*)
  ├── raw_dvf_plus ──────────► stg_dvf__mutations_idf ──────┐
  ├── raw_stats_dvf ─────────► stg_dvf__stats_communales    │
  ├── raw_cog_communes ──────► stg_insee__cog_communes ─────► int_geo__communes_idf ─┐
  ├── raw_filosofi_communes ─► stg_insee__filosofi_communes ┐                        │
  ├── raw_population_* ──────► stg_insee__population_* ─────┼► int_revenus__commune ─┤
  ├── raw_population_age ────► stg_insee__population_age ───┼► int_demo__structure   │
  ├── raw_loyers_communes ──► stg_logement__loyers_communes │   _age_commune ────────┤
  └── raw_delinquance_* ────► stg_logement__delinquance_*   │                        │
                              int_immo__prix_commune_annee ◄┴────────────────────────┘
                                         │
                                         ▼
                          mart_immo__accessibilite_commune
                              │                   │
                              ▼                   ▼
                     mart_immo__synthese_zone   mart_immo__evolution_prix
```

Les modèles `stg_logement__*` sont exportés tels quels vers PostgreSQL par `setup_metabase.py` pour alimenter les cartes loyers / délinquance.

## Clés géographiques

- **Clé principale** : `code_commune` (code INSEE à 5 caractères)
- **Filtrage IDF** : `code_region = '11'` dans le COG, ou `code_departement in ('75','77','78','91','92','93','94','95')`
- **Zones analytiques** : Paris (75), Petite couronne (92, 93, 94), Grande couronne (77, 78, 91, 95)
- **Paris** : code commune `75056`, arrondissements `75101`-`75120` (type ARM dans COG)

### Gestion des divergences de mailles

Les jointures entre sources se font sur `code_commune` (INSEE). Risques :
- Filosofi peut avoir des codes manquants (secret statistique) → `LEFT JOIN` depuis le référentiel géo
- DVF+ utilise le code commune au moment de la transaction → léger décalage possible si fusion récente
- Les tests dbt `relationships` vers `int_geo__communes_idf` détectent les codes orphelins

## Setup

### Prérequis

- Python >= 3.11
- [uv](https://docs.astral.sh/uv/) installé
- [just](https://github.com/casey/just) installé (`brew install just`)
- [Docker](https://docs.docker.com/get-docker/) installé (pour Metabase)

### Installation (dev local)

```bash
# Installer les dépendances Python
just setup

# Télécharger et charger les données (~500 MB, 10 min)
just ingest

# Lancer les transformations et tests dbt
just dbt-all

# Lancer Metabase + configurer le dashboard
just metabase-up
```

### Pipeline complet en une commande

```bash
just all
```

### Commandes individuelles

```bash
# Dépendances
uv sync

# Ingestion
uv run ingest             # Téléchargement + chargement DuckDB
uv run ingest --force     # Forcer le re-téléchargement
uv run ingest --v2        # Inclure les datasets IRIS

# dbt (depuis le dossier dbt/)
cd dbt
uv run dbt deps --profiles-dir .
uv run dbt build --profiles-dir .      # run + test
uv run dbt docs generate --profiles-dir .
uv run dbt docs serve --profiles-dir . # doc sur localhost:8080

# Export CSV
just export               # Exporte les marts dans data/processed/

# Metabase
just metabase-up          # Lance Docker, exporte les marts, configure le dashboard
just metabase-down        # Arrête les containers
```

### Variables de configuration dbt

Seuils de filtrage dans `dbt/dbt_project.yml` (section `vars:`) :

```yaml
vars:
  dvf_prix_min: 1000
  dvf_prix_max: 50000000
  dvf_surface_min: 5
  dvf_surface_max: 5000
  dvf_annee_min: 2018
  communes_nb_ventes_min: 5
```

Référencés dans les modèles via `{{ var('dvf_prix_min') }}` — ne jamais hardcoder.

## Metabase

Le dashboard est accessible sur `http://localhost:3000` après `just metabase-up`.

Architecture de visualisation :
- **DuckDB** reste le moteur analytique (ingestion, dbt)
- **PostgreSQL** (Docker) reçoit les tables marts via l'extension `postgres` de DuckDB
- **Metabase** (Docker) se connecte à PostgreSQL nativement
- **nginx** (Docker) sert les GeoJSON pour les cartes choroplèthes

`scripts/setup_metabase.py` est la **source de vérité du dashboard** : ne jamais éditer via l'UI Metabase. Tout changement passe par le script puis redéploiement. Le dashboard est mis à jour en place (idempotent).

### Credentials (dev)

Par défaut en local :
```
Email    : admin@panorama-idf.local
Password : PanoramaIdf2024!
```

En prod, ces valeurs sont lues depuis `.env` (`MB_ADMIN_EMAIL`, `MB_ADMIN_PASSWORD`, `POSTGRES_PASSWORD`). Voir `.env.example` et `DEPLOYMENT.md`.

## Extension V2 : IRIS / arrondissements

Le projet est conçu pour évoluer vers un grain plus fin :

1. **IRIS** : ajouter `filosofi_iris` (déjà dans la config) + contours IRIS géographiques
2. **Arrondissements Paris** : agréger les IRIS par arrondissement (code IRIS commence par `751xx`)
3. **Grandes communes** : même approche IRIS pour Boulogne, Nanterre, Saint-Denis, etc.

Pour activer :
- `just ingest-v2` télécharge les fichiers IRIS
- Créer `stg_insee__filosofi_iris`, `int_revenus__iris`, `mart_immo__accessibilite_iris`
- Ajouter une table de passage IRIS → arrondissement

## Ce qu'on peut et ne peut pas conclure

### On peut dire

- « Le ratio prix/revenu est X fois plus élevé à [commune A] qu'à [commune B] »
- « La part des 25-39 ans est plus faible dans les communes où le ratio d'accès est le plus élevé »
- « Le prix au m² a augmenté de X% en Y ans dans [zone] »
- « [Commune] se classe Nème sur les communes IDF en termes de tension d'accès »
- « La délinquance par habitant est plus élevée en catégorie X dans [zone] » (relatif, pas causal)

### On ne peut PAS dire

- « Les jeunes ne peuvent plus acheter à [commune] » (on ne sait pas qui achète)
- « Les prix augmentent *à cause de* [facteur] » (pas de modèle causal)
- « Le taux d'effort réel des ménages est de X% » (on n'a pas les revenus des acheteurs)
- « Les primo-accédants sont exclus de [zone] » (DVF ne distingue pas primo/non primo)
- « [Commune] est plus dangereuse que [commune B] » (taux de plainte ≠ criminalité réelle)

## Déploiement en production

Guide détaillé dans [`DEPLOYMENT.md`](./DEPLOYMENT.md).

Stack de prod :
- VPS Hetzner CX22 (4.35 €/mois, 2 vCPU, 4 GB RAM)
- Domaine IONOS .fr (~6 €/an)
- [Caddy](https://caddyserver.com) comme reverse proxy avec HTTPS automatique (Let's Encrypt)
- Mêmes containers PostgreSQL + Metabase + nginx qu'en local, via `docker-compose.prod.yml`
- Ports Postgres et Metabase bindés sur `127.0.0.1` uniquement (pas exposés sur internet)
- Backup quotidien via `scripts/backup.sh` (cron, rotation 7 jours)

Coût total : **~60 €/an** tout compris.

## Stack technique

| Outil | Rôle |
|-------|------|
| **uv** | Gestion Python et dépendances |
| **DuckDB** | Warehouse analytique local |
| **dbt-duckdb** | Transformations SQL, tests, documentation |
| **PostgreSQL** | Base de serving pour Metabase (Docker) |
| **Metabase** | Dashboard et visualisation (Docker) |
| **nginx** | Serveur de GeoJSON pour les cartes Metabase |
| **Caddy** | Reverse proxy + HTTPS automatique (prod) |
| **httpx** | Téléchargement des fichiers |
| **rich** | Affichage console |
| **GitHub Actions** | CI : `dbt build` sur chaque PR |

## Licence

Données sources sous licences ouvertes (Licence Ouverte / Open Licence Etalab, ODbL selon les cas). Vérifier les conditions de chaque source individuellement.
