/*
    int_immo__prix_commune_annee
    ----------------------------
    Agrégation des prix immobiliers par commune IDF et année.

    Stratégie : on utilise les mutations DVF géolocalisées pour calculer
    des médianes et moyennes au niveau commune.

    Grain : commune IDF × année

    On filtre sur les appartements et maisons (biens résidentiels).
    On exclut les transactions à prix aberrant (< 1000€ ou > 50M€).

    IMPORTANT : DVF contient plusieurs lignes par mutation (une par lot/parcelle),
    partageant le même id_mutation et la même valeur_fonciere. On déduplique
    par id_mutation pour éviter de compter plusieurs fois la même transaction.
*/

with communes_idf as (
    select code_commune
    from {{ ref('int_geo__communes_idf') }}
),

-- Déduplique par mutation : une seule ligne par id_mutation,
-- en gardant le type_local et la surface du lot principal (plus grande surface).
mutations_dedup as (
    select distinct on (m.id_mutation)
        m.id_mutation,
        m.code_commune,
        m.annee,
        m.type_local,
        m.valeur_fonciere,
        m.surface_bati

    from {{ ref('stg_dvf__mutations_idf') }} m
    inner join communes_idf c on m.code_commune = c.code_commune
    where
        m.type_local in ('Appartement', 'Maison')
        and m.valeur_fonciere > {{ var('dvf_prix_min') }}
        and m.valeur_fonciere < {{ var('dvf_prix_max') }}
        and m.surface_bati > {{ var('dvf_surface_min') }}
        and m.surface_bati < {{ var('dvf_surface_max') }}
    order by m.id_mutation, m.surface_bati desc
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

    from mutations_dedup
    where annee >= {{ var('dvf_annee_min') }}
    group by code_commune, annee
)

select
    code_commune,
    annee,
    nb_ventes,
    nb_ventes_appartements,
    nb_ventes_maisons,
    prix_median,
    prix_moyen,
    prix_m2_median,
    prix_m2_moyen,
    surface_mediane,
    surface_moyenne
from aggregated
