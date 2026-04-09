/*
    int_revenus__commune
    --------------------
    Revenus, loyers et securite par commune IDF.
    Jointure Filosofi + population + loyers + delinquance.

    Grain : commune IDF

    On enrichit les donnees Filosofi avec la population, les loyers
    et la delinquance pour permettre des analyses multi-dimensionnelles.
*/

with communes_idf as (
    select code_commune, nom_commune, code_departement, zone_idf
    from {{ ref('int_geo__communes_idf') }}
),

filosofi as (
    select * from {{ ref('stg_insee__filosofi_communes') }}
),

population as (
    select code_commune, population_2021
    from {{ ref('stg_insee__population_communes') }}
),

loyers as (
    select code_commune, loyer_m2_median
    from {{ ref('stg_logement__loyers_communes') }}
),

joined as (
    select
        c.code_commune,
        c.nom_commune,
        c.code_departement,
        c.zone_idf,

        -- Revenus Filosofi
        f.niveau_vie_median,
        f.niveau_vie_q1,
        f.niveau_vie_q3,
        f.niveau_vie_d1,
        f.niveau_vie_d9,
        f.indice_gini,
        f.taux_pauvrete_60,
        f.nb_menages_fiscaux,

        -- Population
        p.population_2021,

        -- Loyers
        l.loyer_m2_median,

        -- Ratio d'inegalite interdecile
        round(f.niveau_vie_d9 / nullif(f.niveau_vie_d1, 0), 2) as ratio_interdecile_d9_d1

    from communes_idf c
    left join filosofi f on c.code_commune = f.code_commune
    left join population p on c.code_commune = p.code_commune
    left join loyers l on c.code_commune = l.code_commune
)

select * from joined
