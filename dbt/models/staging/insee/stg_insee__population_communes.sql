/*
    stg_insee__population_communes
    ------------------------------
    Population légale des communes — recensement 2021.
    Source : INSEE base historique de population communale.

    Grain : commune

    Le fichier historique contient des colonnes par année de recensement.
    On extrait uniquement la population 2021.
*/

with source as (
    select * from {{ source('insee', 'raw_population_communes') }}
),

cleaned as (
    select
        trim(cast("CODGEO" as varchar)) as code_commune,
        trim(cast("LIBGEO" as varchar)) as nom_commune,
        cast(nullif(trim(cast("PMUN2021" as varchar)), '') as double) as population_2021,
        cast(nullif(trim(cast("PMUN2015" as varchar)), '') as double) as population_2015

    from source
    where "CODGEO" is not null
)

select * from cleaned
