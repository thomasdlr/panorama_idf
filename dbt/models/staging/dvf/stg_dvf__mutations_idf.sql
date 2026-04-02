/*
    stg_dvf__mutations_idf
    ----------------------
    Nettoyage des mutations DVF+ pour l'Île-de-France.
    Source : Cerema DVF+

    Grain : mutation individuelle

    Notes :
    - DVF+ ne contient PAS l'âge de l'acquéreur.
    - Les colonnes exactes dépendent du millésime DVF+ ; on caste de façon
      défensive avec nullif.
*/

with source as (
    select * from {{ source('dvf', 'raw_dvf_plus') }}
),

cleaned as (
    select
        -- Identifiant mutation
        trim(id_mutation) as id_mutation,
        trim(id_disposition) as id_disposition,

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

        -- Géolocalisation
        cast(nullif(trim(latitude), '') as double) as latitude,
        cast(nullif(trim(longitude), '') as double) as longitude

    from source
    where
        trim(nature_mutation) = 'Vente'
        and code_commune is not null
)

select * from cleaned
