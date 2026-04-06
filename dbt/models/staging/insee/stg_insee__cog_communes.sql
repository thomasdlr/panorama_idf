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

zones as (
    select * from {{ ref('zones_idf') }}
),

with_zone as (
    select
        c.*,
        z.zone_idf,
        c.code_region = '11' as is_idf

    from cleaned c
    left join zones z on c.code_departement = z.code_departement
)

select * from with_zone
