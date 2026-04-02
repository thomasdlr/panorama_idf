# France Aujourd'hui

Analyse de l'accessibilité immobilière en Île-de-France à partir d'open data publics français.

## Objectif

Produire des **indicateurs territoriaux robustes** croisant prix immobiliers, revenus locaux et structure d'âge de la population, pour répondre à des questions comme :

- Où acheter devient le plus difficile en Île-de-France ?
- Comment évoluent les prix immobiliers par rapport aux revenus locaux ?
- Quels territoires gardent une forte présence des 25-39 ans malgré des prix élevés ?
- Comment comparer Paris, petite couronne et grande couronne ?

Le projet est conçu pour alimenter un **site éditorial** (articles, cartes, classements).

## Ce que ce projet n'est PAS

- Il ne prédit pas les prix immobiliers.
- Il ne mesure pas l'âge des acheteurs (DVF ne contient pas cette information en open data).
- Il ne fait aucune affirmation causale (« les prix augmentent *parce que* les jeunes partent »).
- L'angle est : **pression immobilière vs revenus locaux vs structure d'âge des habitants**.

## Limites méthodologiques

| Sujet | Limite | Impact |
|-------|--------|--------|
| **Revenus** | Filosofi mesure des revenus fiscaux et sociaux, pas les revenus réels de tous les ménages. Secret statistique sur petites communes. | Certaines communes manquent de données revenus. |
| **Prix immobiliers** | DVF/DVF+ enregistre les transactions, pas les prix du marché. Biais vers les biens effectivement vendus. | Communes avec peu de ventes = médianes instables (filtrées si < 5 ventes). |
| **Âge** | Le recensement donne la structure d'âge des *résidents*, pas des *acheteurs*. | La part des 25-39 ans est un indicateur de composition locale, pas un proxy de l'accession. |
| **Temporalité** | Filosofi et RP sont fixes (2021), seuls les prix DVF varient par année. | Les ratios prix/revenu sont des proxys, pas des taux d'effort réels. |
| **Géographie** | Le COG évolue chaque année (fusions de communes). V1 au grain commune ; IRIS en V2. | Jointures approximatives possibles sur communes récemment fusionnées. |

## Datasets

| Source | Description | Grain V1 | Producteur |
|--------|-------------|----------|------------|
| **DVF+ (Cerema)** | Transactions immobilières géolocalisées | Mutation | Cerema / DGFiP |
| **Statistiques DVF** | Agrégats prix/volumes prêts à l'emploi | Commune × année | data.gouv.fr |
| **Filosofi 2021** | Revenus, niveau de vie, pauvreté | Commune | INSEE |
| **RP 2021 — Population** | Population légale communale | Commune | INSEE |
| **RP 2021 — Âge** | Population par sexe et âge quinquennal | Commune × âge | INSEE |
| **COG 2024** | Référentiel des communes | Commune | INSEE |

## KPI calculés

| Indicateur | Définition | Interprétation |
|-----------|------------|----------------|
| `ratio_prix_m2_revenu_mensuel` | Prix m² médian / (niveau de vie médian / 12) | Nombre de mois de niveau de vie pour 1 m² |
| `ratio_achat_revenu_annuel` | Prix médian transaction / niveau de vie médian | Nombre d'années de niveau de vie pour un achat médian |
| `ratio_achat_revenu_q1` | Prix médian / niveau de vie Q1 | Idem pour les ménages du 1er quartile (plus modestes) |
| `indice_tension` | Score composite 0-100 | Classement relatif entre communes IDF (pas de seuil absolu) |
| `part_25_39` | Pop 25-39 ans / pop totale | Part des jeunes adultes dans la population résidente |

### Indice de tension — composition

| Composante | Poids | Logique |
|-----------|-------|---------|
| Ratio achat/revenu annuel | 40% | Effort d'achat pur |
| Ratio m²/revenu mensuel | 30% | Densité de prix |
| Faible présence 25-39 ans | 15% | Fuite des jeunes adultes |
| Taux de pauvreté | 15% | Fragilité économique |

Chaque composante est normalisée en percentile rank puis pondérée. Score final = percentile composite × 100.

## Architecture du projet

```
france_aujourdhui/
├── pyproject.toml          # Dépendances Python (uv)
├── justfile                # Commandes de lancement (just)
├── README.md
├── .gitignore
│
├── src/france_aujourdhui/
│   └── ingest/             # Scripts de téléchargement et chargement
│       ├── config.py       # URLs et métadonnées des datasets
│       ├── download.py     # Téléchargement et extraction
│       ├── prepare.py      # Chargement dans DuckDB
│       └── cli.py          # Point d'entrée CLI
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   │
│   ├── models/
│   │   ├── staging/
│   │   │   ├── dvf/        # stg_dvf__stats_communales, stg_dvf__mutations_idf
│   │   │   └── insee/      # stg_insee__cog_communes, filosofi, population, âge
│   │   │
│   │   ├── intermediate/
│   │   │   ├── int_geo__communes_idf         # Référentiel communes IDF
│   │   │   ├── int_immo__prix_commune_annee  # Prix agrégés commune × année
│   │   │   ├── int_revenus__commune          # Revenus + population
│   │   │   └── int_demo__structure_age_commune  # Structure d'âge
│   │   │
│   │   └── marts/
│   │       ├── mart_immo__accessibilite_commune  # Table analytique principale
│   │       ├── mart_immo__ranking_tension         # Classement par indice
│   │       ├── mart_immo__synthese_zone           # Synthèse Paris/PC/GC
│   │       └── mart_immo__evolution_prix          # Évolution temporelle
│   │
│   ├── macros/             # cast_filosofi_numeric, filter_idf, safe_divide
│   └── tests/              # Tests singuliers (cohérence prix, ratios)
│
└── data/
    ├── raw/                # Fichiers téléchargés (non versionnés)
    ├── processed/          # Exports CSV (non versionnés)
    └── france_aujourdhui.duckdb  # Warehouse local
```

## Lineage dbt

```
Sources (DuckDB)
  ├── raw_dvf_plus ──────────► stg_dvf__mutations_idf ──┐
  ├── raw_stats_dvf ─────────► stg_dvf__stats_communales │
  ├── raw_cog_communes ──────► stg_insee__cog_communes ──► int_geo__communes_idf ─┐
  ├── raw_filosofi_communes ─► stg_insee__filosofi_communes ─┐                    │
  ├── raw_population_communes► stg_insee__population_communes├► int_revenus__commune┤
  └── raw_population_age ────► stg_insee__population_age ────► int_demo__structure  │
                                                               _age_commune ────────┤
                              int_immo__prix_commune_annee ◄───────────────────────┘
                                         │
                                         ▼
                          mart_immo__accessibilite_commune
                              │         │          │
                              ▼         ▼          ▼
                    ranking_tension  synthese_zone  evolution_prix
```

## Clés géographiques

- **Clé principale** : `code_commune` (code INSEE à 5 caractères)
- **Filtrage IDF** : via `code_region = '11'` dans le COG, ou `code_departement in ('75','77','78','91','92','93','94','95')`
- **Zones analytiques** : Paris (75), Petite couronne (92, 93, 94), Grande couronne (77, 78, 91, 95)
- **Paris** : code commune `75056` (commune), arrondissements `75101`-`75120` (type ARM dans COG)

### Gestion des divergences de mailles

Les jointures entre sources se font sur `code_commune` (INSEE). Risques :
- Filosofi peut avoir des codes manquants (secret statistique) → `LEFT JOIN` depuis le référentiel géo
- DVF+ utilise le code commune au moment de la transaction → léger décalage possible si fusion récente
- Population par âge utilise le code commune RP → cohérent avec Filosofi

## Setup

### Prérequis

- Python >= 3.11
- [uv](https://docs.astral.sh/uv/) installé
- [just](https://github.com/casey/just) installé (`brew install just`)

### Installation

```bash
# Installer les dépendances Python
just setup

# Télécharger et charger les données
just ingest

# Lancer les transformations et tests dbt
just dbt-all
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

# dbt
cd dbt
uv run dbt deps           # Installer dbt_utils
uv run dbt run            # Exécuter les transformations
uv run dbt test           # Lancer les tests
uv run dbt docs generate  # Générer la documentation
uv run dbt docs serve     # Servir la doc sur localhost:8080

# Export CSV
just export               # Exporte les marts dans data/processed/
```

## Extension V2 : Paris / IRIS / arrondissements

Le projet est conçu pour évoluer vers un grain plus fin :

1. **IRIS** : ajouter `filosofi_iris` (déjà dans la config) + contours IRIS géographiques
2. **Arrondissements Paris** : agréger les IRIS par arrondissement (code IRIS commence par `751xx`)
3. **Grandes communes** : même approche IRIS pour Boulogne, Nanterre, Saint-Denis, etc.

Pour activer :
- `just ingest-v2` télécharge les fichiers IRIS
- Créer des modèles `stg_insee__filosofi_iris`, `int_revenus__iris`, `mart_immo__accessibilite_iris`
- Ajouter une table de passage IRIS → arrondissement

## Ce qu'on peut et ne peut pas conclure

### On peut dire

- « Le ratio prix/revenu est X fois plus élevé à [commune A] qu'à [commune B] »
- « La part des 25-39 ans est plus faible dans les communes où le ratio d'accès est le plus élevé »
- « Le prix au m² a augmenté de X% en Y ans dans [zone] »
- « [Commune] se classe Nème sur les communes IDF en termes de tension d'accès »

### On ne peut PAS dire

- « Les jeunes ne peuvent plus acheter à [commune] » (on ne sait pas qui achète)
- « Les prix augmentent *à cause de* [facteur] » (pas de modèle causal)
- « Le taux d'effort réel des ménages est de X% » (on n'a pas les revenus des acheteurs)
- « Les primo-accédants sont exclus de [zone] » (DVF ne distingue pas primo/non primo)

## Stack technique

| Outil | Rôle |
|-------|------|
| **uv** | Gestion Python et dépendances |
| **DuckDB** | Warehouse analytique local |
| **dbt-duckdb** | Transformations SQL, tests, documentation |
| **httpx** | Téléchargement des fichiers |
| **pandas / pyarrow** | Manipulation ponctuelle si nécessaire |
| **rich** | Affichage console |

## Licence

Données sources sous licences ouvertes (Licence Ouverte / Open Licence Etalab, ODbL selon les cas). Vérifier les conditions de chaque source individuellement.
