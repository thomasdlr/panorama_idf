/*
    stg_logement__delinquance_detail
    --------------------------------
    Delinquance par commune, annee et type de delit.
    Garde le detail par indicateur (contrairement au modele agrege).

    Grain : commune x annee x indicateur
*/

with source as (
    select * from {{ source('logement', 'raw_delinquance_communes') }}
),

cleaned as (
    select
        trim("CODGEO_2025") as code_commune,
        cast(trim("annee") as integer) as annee,
        trim("indicateur") as type_delit,
        {{ cast_filosofi_numeric('"nombre"') }} as nb_faits,
        {{ cast_filosofi_numeric('"taux_pour_mille"') }} as taux_pour_mille,
        {{ cast_filosofi_numeric('"insee_pop"') }} as population

    from source
    where
        nullif(trim("CODGEO_2025"), '') is not null
        and trim("est_diffuse") = 'diff'
        and {{ cast_filosofi_numeric('"nombre"') }} is not null
)

select * from cleaned
