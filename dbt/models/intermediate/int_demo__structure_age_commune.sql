/*
    int_demo__structure_age_commune
    -------------------------------
    Structure d'âge agrégée par commune IDF.

    Calcule la population totale et la part de chaque grande tranche d'âge.
    Focus particulier sur les 25-39 ans (proxy de la tranche d'âge
    la plus active sur le marché immobilier d'accession).

    Grain : commune IDF

    Limites :
    - Données RP 2021, pas de série temporelle ici.
    - La structure d'âge locale ne dit rien sur qui achète réellement.
    - C'est un indicateur de composition de la population résidente.
*/

with communes_idf as (
    select code_commune
    from {{ ref('int_geo__communes_idf') }}
),

population as (
    select
        p.code_commune,
        p.tranche_age_quinquennale,
        sum(p.effectif) as effectif

    from {{ ref('stg_insee__population_age') }} p
    inner join communes_idf c on p.code_commune = c.code_commune
    group by p.code_commune, p.tranche_age_quinquennale
),

totals as (
    select
        code_commune,
        sum(effectif) as population_totale
    from population
    group by code_commune
),

age_groups as (
    select
        p.code_commune,
        t.population_totale,

        -- Jeunes adultes 18-24
        sum(case
            when p.tranche_age_quinquennale in ('15-19', '20-24') then p.effectif
            else 0
        end) as pop_18_24_approx,

        -- 25-39 ans (tranche cible)
        sum(case
            when p.tranche_age_quinquennale in ('25-29', '30-34', '35-39') then p.effectif
            else 0
        end) as pop_25_39,

        -- 40-59 ans
        sum(case
            when p.tranche_age_quinquennale in ('40-44', '45-49', '50-54', '55-59') then p.effectif
            else 0
        end) as pop_40_59,

        -- 60+ ans
        sum(case
            when p.tranche_age_quinquennale in (
                '60-64', '65-69', '70-74', '75-79', '80-84', '85-89', '90-94', '95+'
            ) then p.effectif
            else 0
        end) as pop_60_plus

    from population p
    inner join totals t on p.code_commune = t.code_commune
    group by p.code_commune, t.population_totale
),

with_ratios as (
    select
        *,
        round(pop_25_39 / nullif(population_totale, 0), 4) as part_25_39,
        round(pop_18_24_approx / nullif(population_totale, 0), 4) as part_18_24_approx,
        round(pop_40_59 / nullif(population_totale, 0), 4) as part_40_59,
        round(pop_60_plus / nullif(population_totale, 0), 4) as part_60_plus

    from age_groups
)

select * from with_ratios
