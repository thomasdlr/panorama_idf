/*
    stg_logement__delinquance_communes
    ----------------------------------
    Delinquance enregistree par commune et annee.
    Source : SSMSI / Ministere de l'Interieur.

    On agrege les differents indicateurs en un taux global pour 1000 habitants.
    Seuls les faits diffuses (est_diffuse = 'diff') sont comptabilises.

    Grain : commune x annee
*/

with source as (
    select * from {{ source('logement', 'raw_delinquance_communes') }}
),

cleaned as (
    select
        trim("CODGEO_2025") as code_commune,
        cast(trim("annee") as integer) as annee,
        trim("indicateur") as indicateur,
        trim("est_diffuse") as est_diffuse,
        {{ cast_filosofi_numeric('"nombre"') }} as nombre,
        {{ cast_filosofi_numeric('"taux_pour_mille"') }} as taux_pour_mille,
        {{ cast_filosofi_numeric('"insee_pop"') }} as population

    from source
    where
        nullif(trim("CODGEO_2025"), '') is not null
        and trim("est_diffuse") = 'diff'
),

-- Agrege tous les types de delit en un total par commune x annee
aggregated as (
    select
        code_commune,
        annee,
        sum(nombre) as nb_faits_total,
        max(population) as population,
        round(sum(nombre) / nullif(max(population), 0) * 1000, 1) as taux_delinquance_pour_mille

    from cleaned
    group by code_commune, annee
)

select * from aggregated
