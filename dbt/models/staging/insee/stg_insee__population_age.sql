/*
    stg_insee__population_age
    -------------------------
    Population par sexe et tranche d'âge quinquennale, par commune (RP 2021).
    Source : INSEE — BTT_TD_POP1B_2021

    Grain : commune × sexe × tranche d'âge

    Structure typique du fichier :
    - CODGEO : code commune
    - SEXE : 1 = hommes, 2 = femmes
    - AGED100 : âge détaillé ou AGEQ100 : tranche quinquennale
    - NB : effectif

    Note : la structure exacte peut varier selon le millésime.
    On agrège par commune et tranche d'âge pour la suite.
*/

with source as (
    select * from {{ source('insee', 'raw_population_age') }}
),

cleaned as (
    select
        trim("CODGEO") as code_commune,
        trim("SEXE") as sexe,
        trim("AGED100") as age_revolu,
        cast(nullif(trim("NB"), '') as double) as effectif

    from source
    where
        "CODGEO" is not null
        and "NB" is not null
),

-- Agrégation par tranche quinquennale
with_tranche as (
    select
        code_commune,
        sexe,
        age_revolu,
        cast(age_revolu as integer) as age_int,
        effectif,

        case
            when cast(age_revolu as integer) between 0 and 4 then '00-04'
            when cast(age_revolu as integer) between 5 and 9 then '05-09'
            when cast(age_revolu as integer) between 10 and 14 then '10-14'
            when cast(age_revolu as integer) between 15 and 19 then '15-19'
            when cast(age_revolu as integer) between 20 and 24 then '20-24'
            when cast(age_revolu as integer) between 25 and 29 then '25-29'
            when cast(age_revolu as integer) between 30 and 34 then '30-34'
            when cast(age_revolu as integer) between 35 and 39 then '35-39'
            when cast(age_revolu as integer) between 40 and 44 then '40-44'
            when cast(age_revolu as integer) between 45 and 49 then '45-49'
            when cast(age_revolu as integer) between 50 and 54 then '50-54'
            when cast(age_revolu as integer) between 55 and 59 then '55-59'
            when cast(age_revolu as integer) between 60 and 64 then '60-64'
            when cast(age_revolu as integer) between 65 and 69 then '65-69'
            when cast(age_revolu as integer) between 70 and 74 then '70-74'
            when cast(age_revolu as integer) between 75 and 79 then '75-79'
            when cast(age_revolu as integer) between 80 and 84 then '80-84'
            when cast(age_revolu as integer) between 85 and 89 then '85-89'
            when cast(age_revolu as integer) between 90 and 94 then '90-94'
            when cast(age_revolu as integer) >= 95 then '95+'
            else 'inconnu'
        end as tranche_age_quinquennale

    from cleaned
    where age_revolu != 'ENS'  -- Exclure la ligne total si présente
)

select * from with_tranche
