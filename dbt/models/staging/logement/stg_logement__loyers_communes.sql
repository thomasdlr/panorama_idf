/*
    stg_logement__loyers_communes
    -----------------------------
    Loyers predits au m2 par commune (appartements).
    Source : carte des loyers 2025 (ANIL / Ministere Transition ecologique).

    Grain : commune
    Le champ loypredm2 utilise la virgule comme separateur decimal.
*/

with source as (
    select * from {{ source('logement', 'raw_loyers_communes') }}
),

cleaned as (
    select
        trim("INSEE_C") as code_commune,
        trim("LIBGEO") as nom_commune,
        trim("DEP") as code_departement,
        {{ cast_filosofi_numeric('"loypredm2"') }} as loyer_m2_median,
        {{ cast_filosofi_numeric('"lwr.IPm2"') }} as loyer_m2_borne_basse,
        {{ cast_filosofi_numeric('"upr.IPm2"') }} as loyer_m2_borne_haute,
        trim("TYPPRED") as type_prediction,
        cast(nullif(trim("nbobs_com"), '') as integer) as nb_observations_commune

    from source
    where nullif(trim("INSEE_C"), '') is not null
)

select * from cleaned
