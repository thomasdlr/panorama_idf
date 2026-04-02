/*
    mart_immo__ranking_tension
    ==========================
    Classement des communes IDF par tension d'accès immobilier.

    Construit un indice de tension composite à partir de :
    1. ratio_achat_revenu_annuel (poids 40%) — effort d'achat pur
    2. ratio_prix_m2_revenu_mensuel (poids 30%) — densité de prix
    3. inverse de part_25_39 (poids 15%) — faible présence jeunes adultes
    4. taux_pauvrete_60 (poids 15%) — fragilité économique

    L'indice est normalisé entre 0 et 100 (percentile rank).
    Plus l'indice est élevé, plus la commune semble "tendue".

    Grain : commune IDF × année (dernière année disponible priorisée)

    IMPORTANT :
    - Cet indice est un outil de CLASSEMENT RELATIF entre communes IDF.
    - Il ne mesure pas une difficulté absolue d'achat.
    - Il ne dit rien sur les acheteurs réels ni leur profil.
    - C'est un proxy composite, transparent et reproductible.
*/

with base as (
    select * from {{ ref('mart_immo__accessibilite_commune') }}
),

-- On calcule les percentile ranks pour normaliser chaque composante
ranked as (
    select
        *,

        -- Plus le ratio est élevé, plus c'est tendu → percentile direct
        percent_rank() over (
            partition by annee order by ratio_achat_revenu_annuel
        ) as prank_ratio_achat,

        percent_rank() over (
            partition by annee order by ratio_prix_m2_revenu_mensuel
        ) as prank_ratio_m2,

        -- Plus la part 25-39 est FAIBLE, plus c'est tendu → inverse
        percent_rank() over (
            partition by annee order by part_25_39 desc
        ) as prank_faible_jeunes,

        -- Plus le taux de pauvreté est élevé, plus c'est fragile
        percent_rank() over (
            partition by annee order by taux_pauvrete_60
        ) as prank_pauvrete

    from base
    where
        ratio_achat_revenu_annuel is not null
        and part_25_39 is not null
),

scored as (
    select
        *,

        -- Indice composite pondéré (0-100)
        round(
            (
                0.40 * prank_ratio_achat
                + 0.30 * prank_ratio_m2
                + 0.15 * prank_faible_jeunes
                + 0.15 * prank_pauvrete
            ) * 100,
            1
        ) as indice_tension,

        -- Rang par année
        row_number() over (
            partition by annee order by
                0.40 * prank_ratio_achat
                + 0.30 * prank_ratio_m2
                + 0.15 * prank_faible_jeunes
                + 0.15 * prank_pauvrete
            desc
        ) as rang_tension

    from ranked
)

select
    code_commune,
    nom_commune,
    code_departement,
    zone_idf,
    annee,
    population_2021,
    nb_ventes,
    prix_m2_median,
    prix_median,
    niveau_vie_median,
    part_25_39,
    taux_pauvrete_60,
    ratio_prix_m2_revenu_mensuel,
    ratio_achat_revenu_annuel,
    ratio_achat_revenu_q1,
    indice_tension,
    rang_tension

from scored
