/*
    stg_dvf__stats_communales
    -------------------------
    Nettoyage et standardisation des statistiques DVF agrégées.
    Source : data.gouv.fr — Statistiques DVF

    Grain : entité géographique (commune, EPCI, département) — tous types
    de biens confondus sur la période complète (pas de dimension année).

    Le dataset fournit des agrégats pré-calculés par type de bien :
    appartement, maison, apt+maison, local commercial.
*/

with source as (
    select * from {{ source('dvf', 'raw_stats_dvf') }}
),

cleaned as (
    select
        -- Clés géographiques
        trim(code_geo) as code_geo,
        trim(libelle_geo) as libelle_geo,
        trim(code_parent) as code_parent,
        trim(echelle_geo) as echelle_geo,

        -- Appartements
        cast(nullif(trim(nb_ventes_whole_appartement), '') as integer) as nb_ventes_appartement,
        cast(nullif(trim(moy_prix_m2_whole_appartement), '') as double) as prix_m2_moyen_appartement,
        cast(nullif(trim(med_prix_m2_whole_appartement), '') as double) as prix_m2_median_appartement,

        -- Maisons
        cast(nullif(trim(nb_ventes_whole_maison), '') as integer) as nb_ventes_maison,
        cast(nullif(trim(moy_prix_m2_whole_maison), '') as double) as prix_m2_moyen_maison,
        cast(nullif(trim(med_prix_m2_whole_maison), '') as double) as prix_m2_median_maison,

        -- Appartements + Maisons
        cast(nullif(trim(nb_ventes_whole_apt_maison), '') as integer) as nb_ventes_apt_maison,
        cast(nullif(trim(moy_prix_m2_whole_apt_maison), '') as double) as prix_m2_moyen_apt_maison,
        cast(nullif(trim(med_prix_m2_whole_apt_maison), '') as double) as prix_m2_median_apt_maison,

        -- Locaux commerciaux
        cast(nullif(trim(nb_ventes_whole_local), '') as integer) as nb_ventes_local,
        cast(nullif(trim(moy_prix_m2_whole_local), '') as double) as prix_m2_moyen_local,
        cast(nullif(trim(med_prix_m2_whole_local), '') as double) as prix_m2_median_local

    from source
    where code_geo is not null
)

select * from cleaned
