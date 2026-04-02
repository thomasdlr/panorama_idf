/*
    mart_immo__evolution_prix
    =========================
    Évolution temporelle des prix par commune IDF.
    Calcule les variations annuelles et cumulées.

    Grain : commune IDF × année

    Utile pour les articles "où les prix ont le plus augmenté/baissé".
*/

with prix as (
    select * from {{ ref('int_immo__prix_commune_annee') }}
),

geo as (
    select code_commune, nom_commune, code_departement, zone_idf
    from {{ ref('int_revenus__commune') }}
),

with_lag as (
    select
        g.code_commune,
        g.nom_commune,
        g.code_departement,
        g.zone_idf,
        p.annee,
        p.nb_ventes,
        p.prix_m2_median,
        p.prix_median,

        lag(p.prix_m2_median) over (
            partition by p.code_commune order by p.annee
        ) as prix_m2_median_annee_prec,

        lag(p.prix_median) over (
            partition by p.code_commune order by p.annee
        ) as prix_median_annee_prec

    from prix p
    inner join geo g on p.code_commune = g.code_commune
    where p.nb_ventes >= 5
),

with_evolution as (
    select
        *,

        -- Variation annuelle prix m²
        round(
            (prix_m2_median - prix_m2_median_annee_prec)
            / nullif(prix_m2_median_annee_prec, 0) * 100,
            1
        ) as variation_prix_m2_pct,

        -- Variation annuelle prix médian
        round(
            (prix_median - prix_median_annee_prec)
            / nullif(prix_median_annee_prec, 0) * 100,
            1
        ) as variation_prix_median_pct

    from with_lag
)

select * from with_evolution
