/*
    int_immo__prix_commune_annee
    ----------------------------
    Agrégation des prix immobiliers par commune IDF et année.

    Stratégie : on utilise prioritairement les mutations DVF+ pour calculer
    des médianes et moyennes au niveau commune, car elles offrent plus de
    contrôle que les stats DVF pré-agrégées.

    Grain : commune IDF × année

    On filtre sur les appartements et maisons (biens résidentiels).
    On exclut les transactions à prix aberrant (< 1000€ ou > 50M€).
*/

with communes_idf as (
    select code_commune
    from {{ ref('int_geo__communes_idf') }}
),

mutations as (
    select
        m.code_commune,
        m.annee,
        m.type_local,
        m.valeur_fonciere,
        m.surface_bati

    from {{ ref('stg_dvf__mutations_idf') }} m
    inner join communes_idf c on m.code_commune = c.code_commune
    where
        m.type_local in ('Appartement', 'Maison')
        and m.valeur_fonciere > 1000
        and m.valeur_fonciere < 50000000
        and m.surface_bati > 5
        and m.surface_bati < 5000
),

aggregated as (
    select
        code_commune,
        annee,

        -- Volume
        count(*) as nb_ventes,
        count(case when type_local = 'Appartement' then 1 end) as nb_ventes_appartements,
        count(case when type_local = 'Maison' then 1 end) as nb_ventes_maisons,

        -- Prix
        median(valeur_fonciere) as prix_median,
        avg(valeur_fonciere) as prix_moyen,

        -- Prix au m²
        median(valeur_fonciere / nullif(surface_bati, 0)) as prix_m2_median,
        avg(valeur_fonciere / nullif(surface_bati, 0)) as prix_m2_moyen,

        -- Surface
        median(surface_bati) as surface_mediane,
        avg(surface_bati) as surface_moyenne

    from mutations
    where annee >= 2018  -- On garde 5 ans de données
    group by code_commune, annee
)

select * from aggregated
