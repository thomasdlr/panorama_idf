/*
    stg_insee__cog_communes
    -----------------------
    Référentiel géographique des communes (COG 2024).
    Utilisé pour :
    - filtrer l'Île-de-France (région 11)
    - obtenir les libellés et rattachements département/région
    - définir Paris / petite couronne / grande couronne

    Grain : commune
*/

with source as (
    select * from {{ source('insee', 'raw_cog_communes') }}
),

cleaned as (
    select
        trim("COM") as code_commune,
        trim("LIBELLE") as nom_commune,
        trim("DEP") as code_departement,
        trim("REG") as code_region,
        trim("TYPECOM") as type_commune,
        trim("COMPARENT") as code_commune_parent

    from source
    where
        -- On garde les communes actuelles (COM) et les arrondissements municipaux (ARM)
        trim("TYPECOM") in ('COM', 'ARM')
),

with_zone as (
    select
        *,
        -- Classification territoriale IDF
        case
            when code_departement = '75' then 'Paris'
            when code_departement in ('92', '93', '94') then 'Petite couronne'
            when code_departement in ('77', '78', '91', '95') then 'Grande couronne'
        end as zone_idf,

        -- Flag IDF
        code_region = '11' as is_idf

    from cleaned
)

select * from with_zone
