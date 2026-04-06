/*
    stg_insee__filosofi_communes
    ----------------------------
    Indicateurs Filosofi 2021 — revenus et niveau de vie par commune.

    Source : INSEE Filosofi 2021 — fichier DEC (déciles) commune
    Grain : commune

    Limites documentées :
    - Données fiscales et sociales, pas d'observation directe de tous les revenus.
    - Secret statistique : certaines valeurs masquées pour les petites communes.
    - Année fiscale 2021 (revenus 2020 pour partie).
    - Le fichier DEC ne contient pas le taux de pauvreté (TP6021).
      On utilise la part des ménages non imposés (1 - PMIMP21/100) comme proxy.

    Nomenclature Filosofi DEC :
    - Q121, Q221, Q321 : quartiles du niveau de vie (Q2 = médiane)
    - D121..D921 : déciles du niveau de vie
    - GI21 : indice de Gini
    - PMIMP21 : part des ménages fiscaux imposés (%)
    - NBMEN21 : nombre de ménages
*/

with source as (
    select * from {{ source('insee', 'raw_filosofi_communes') }}
),

cleaned as (
    select
        trim("CODGEO") as code_commune,

        -- Niveau de vie (en euros annuels) — Q221 = médiane
        {{ cast_filosofi_numeric('"Q221"') }} as niveau_vie_median,
        {{ cast_filosofi_numeric('"Q121"') }} as niveau_vie_q1,
        {{ cast_filosofi_numeric('"Q321"') }} as niveau_vie_q3,
        {{ cast_filosofi_numeric('"D121"') }} as niveau_vie_d1,
        {{ cast_filosofi_numeric('"D921"') }} as niveau_vie_d9,

        -- Inégalités
        {{ cast_filosofi_numeric('"GI21"') }} as indice_gini,

        -- Proxy pauvreté : part des ménages non imposés
        -- Le fichier DEC ne contient pas TP6021 ; on utilise (100 - PMIMP21)
        round(100.0 - {{ cast_filosofi_numeric('"PMIMP21"') }}, 1) as taux_pauvrete_60,

        -- Nombre de ménages
        {{ cast_filosofi_numeric('"NBMEN21"') }} as nb_menages_fiscaux

    from source
    where "CODGEO" is not null
)

select * from cleaned
