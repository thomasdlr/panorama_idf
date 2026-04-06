/*
    stg_dvf__mutations_idf
    ----------------------
    Nettoyage des mutations DVF geolocalisees pour l'Ile-de-France.
    Source : geo-dvf (data.gouv.fr)

    Grain : mutation individuelle
    Materialisation : incremental par annee de mutation.

    Notes :
    - DVF ne contient PAS l'age de l'acquereur.
    - On caste de facon defensive avec nullif pour gerer les champs vides.
*/

{{
    config(
        materialized='incremental',
        unique_key='id_mutation',
        on_schema_change='append_new_columns'
    )
}}

with source as (
    select * from {{ source('dvf', 'raw_dvf_plus') }}

    {% if is_incremental() %}
    where cast(date_mutation as date) > (select max(date_mutation) from {{ this }})
    {% endif %}
),

cleaned as (
    select
        -- Identifiant mutation
        trim(id_mutation) as id_mutation,
        trim(numero_disposition) as numero_disposition,

        -- Date et nature
        cast(date_mutation as date) as date_mutation,
        extract(year from cast(date_mutation as date)) as annee,
        trim(nature_mutation) as nature_mutation,

        -- Localisation
        trim(code_commune) as code_commune,
        trim(nom_commune) as nom_commune,
        trim(code_departement) as code_departement,
        trim(code_postal) as code_postal,

        -- Bien
        trim(type_local) as type_local,
        cast(nullif(trim(nombre_pieces_principales), '') as integer) as nb_pieces,
        cast(nullif(trim(surface_reelle_bati), '') as double) as surface_bati,
        cast(nullif(trim(surface_terrain), '') as double) as surface_terrain,

        -- Prix
        cast(nullif(trim(valeur_fonciere), '') as double) as valeur_fonciere,

        -- Geolocalisation
        cast(nullif(trim(latitude), '') as double) as latitude,
        cast(nullif(trim(longitude), '') as double) as longitude

    from source
    where
        trim(nature_mutation) = 'Vente'
        and nullif(trim(code_commune), '') is not null
)

select * from cleaned
