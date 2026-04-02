/*
    stg_insee__filosofi_communes
    ----------------------------
    Indicateurs Filosofi 2021 — revenus et niveau de vie par commune.

    Source : INSEE Filosofi 2021 — fichier base commune
    Grain : commune

    Limites documentées :
    - Données fiscales et sociales, pas d'observation directe de tous les revenus.
    - Secret statistique : certaines valeurs masquées pour les petites communes.
    - Année fiscale 2021 (revenus 2020 pour partie).

    Nomenclature Filosofi (colonnes fréquentes) :
    - MED21 : médiane du niveau de vie
    - Q121, Q321 : 1er et 3e quartile du niveau de vie
    - D121..D921 : déciles du niveau de vie
    - GI21 : indice de Gini
    - TP6021 : taux de pauvreté seuil 60%
*/

with source as (
    select * from {{ source('insee', 'raw_filosofi_communes') }}
),

cleaned as (
    select
        trim("CODGEO") as code_commune,

        -- Niveau de vie (en euros annuels)
        {{ cast_filosofi_numeric('"MED21"') }} as niveau_vie_median,
        {{ cast_filosofi_numeric('"Q121"') }} as niveau_vie_q1,
        {{ cast_filosofi_numeric('"Q321"') }} as niveau_vie_q3,
        {{ cast_filosofi_numeric('"D121"') }} as niveau_vie_d1,
        {{ cast_filosofi_numeric('"D921"') }} as niveau_vie_d9,

        -- Inégalités
        {{ cast_filosofi_numeric('"GI21"') }} as indice_gini,

        -- Pauvreté
        {{ cast_filosofi_numeric('"TP6021"') }} as taux_pauvrete_60,

        -- Nombre de ménages fiscaux
        {{ cast_filosofi_numeric('"NBMENFISC21"') }} as nb_menages_fiscaux,

        -- Revenu disponible médian par UC (si disponible)
        {{ cast_filosofi_numeric('"MED21"') }} as revenu_disponible_median_uc

    from source
    where "CODGEO" is not null
)

select * from cleaned
