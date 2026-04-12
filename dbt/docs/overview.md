{% docs __overview__ %}

# France Aujourd'hui — Panorama Ile-de-France

Projet d'analyse du cadre de vie en Ile-de-France croisant immobilier, revenus, démographie, sécurité, éducation et mobilité.

## Sources de données

| Source | Fournisseur | Grain | Fréquence |
|--------|-------------|-------|-----------|
| DVF+ | Cerema / data.gouv.fr | Mutation individuelle | Semestriel (2020-2025) |
| Filosofi | INSEE | Commune | Fixe (2021) |
| Population | INSEE RP | Commune × âge | Fixe (2021) |
| COG | INSEE | Commune | Annuel (2024) |
| Loyers | ANIL / Min. Transition écologique | Commune | Fixe (2025) |
| Délinquance | SSMSI / Min. Intérieur | Commune × année | Annuel (2016+) |

## Architecture dbt

```
sources (raw_*)          → données brutes dans DuckDB
  ↓
staging (stg_*)          → nettoyage, typage, filtrage
  ↓
intermediate (int_*)     → jointures, agrégations thématiques
  ↓
marts (mart_*)           → tables analytiques finales
  ↓
Metabase (PostgreSQL)    → dashboard « Panorama Ile-de-France »
```

## Modèles clés

- **mart_immo__accessibilite_commune** : table principale (commune × année), croise prix, revenus, démographie, loyers et délinquance
- **mart_immo__synthese_zone** : agrégats pondérés par zone (Paris / PC / GC)
- **mart_immo__evolution_prix** : variations annuelles de prix par commune

## Seuils de filtrage

Les seuils sont configurables dans `dbt_project.yml` (section `vars:`) :
- Prix : entre 1 000 € et 50 000 000 €
- Surface : entre 5 m² et 5 000 m²
- Année minimale : 2018
- Nombre minimum de ventes par commune : 5

## Limites connues

- Filosofi, population et loyers sont des instantanés fixes (2021/2025) — seuls les prix DVF et la délinquance varient par année
- DVF ne contient pas l'âge de l'acquéreur
- Secret statistique : communes avec < 6 faits de délinquance sont masquées par le SSMSI
- L'indice de pauvreté (taux_pauvrete_60) est un proxy fiscal, pas une mesure directe

{% enddocs %}
