/*
    mart_immo__synthese_zone
    ========================
    Synthèse agrégée par zone IDF : Paris, petite couronne, grande couronne.
    Permet la comparaison macro entre les trois zones.

    Grain : zone IDF × année
*/

with base as (
    select * from {{ ref('mart_immo__accessibilite_commune') }}
),

aggregated as (
    select
        zone_idf,
        annee,

        -- Population
        sum(population_2021) as population_totale,
        count(*) as nb_communes,

        -- Prix
        sum(nb_ventes) as nb_ventes_total,
        -- Moyenne pondérée par le nombre de ventes
        round(
            sum(prix_m2_median * nb_ventes) / nullif(sum(nb_ventes), 0),
            0
        ) as prix_m2_median_pondere,
        round(
            sum(prix_median * nb_ventes) / nullif(sum(nb_ventes), 0),
            0
        ) as prix_median_pondere,

        -- Revenus (moyenne pondérée par population)
        round(
            sum(niveau_vie_median * population_2021) / nullif(sum(population_2021), 0),
            0
        ) as niveau_vie_median_pondere,

        -- Démographie (moyenne pondérée par population)
        round(
            sum(part_25_39 * population_2021) / nullif(sum(population_2021), 0),
            4
        ) as part_25_39_ponderee,

        -- Ratios (moyenne pondérée par ventes)
        round(
            sum(ratio_achat_revenu_annuel * nb_ventes) / nullif(sum(nb_ventes), 0),
            1
        ) as ratio_achat_revenu_pondere,

        round(
            sum(ratio_prix_m2_revenu_mensuel * nb_ventes) / nullif(sum(nb_ventes), 0),
            1
        ) as ratio_m2_revenu_pondere

    from base
    group by zone_idf, annee
)

select * from aggregated
order by annee, zone_idf
