/*
    stg_dvf__stats_communales
    -------------------------
    Nettoyage et standardisation des statistiques DVF agrégées.
    Source : data.gouv.fr — Statistiques DVF

    Grain : commune × année × type de bien
*/

with source as (
    select * from {{ source('dvf', 'raw_stats_dvf') }}
),

cleaned as (
    select
        -- Clés
        trim(code_commune) as code_commune,
        cast(annee_mutation as integer) as annee,
        trim(libelle_nature_mutation) as nature_mutation,
        trim(type_local) as type_local,

        -- Métriques prix
        cast(nullif(trim(prix_m2_median), '') as double) as prix_m2_median,
        cast(nullif(trim(prix_m2_moyen), '') as double) as prix_m2_moyen,

        -- Volumes
        cast(nullif(trim(nb_mutations), '') as integer) as nb_mutations,
        cast(nullif(trim(nb_locaux), '') as integer) as nb_locaux,

        -- Surface
        cast(nullif(trim(surface_median), '') as double) as surface_mediane,
        cast(nullif(trim(prix_median), '') as double) as prix_median

    from source
    where
        -- On ne garde que les ventes
        trim(libelle_nature_mutation) = 'Vente'
        -- Filtre colonnes existantes
        and code_commune is not null
)

select * from cleaned
