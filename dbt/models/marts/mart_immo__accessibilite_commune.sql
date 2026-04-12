/*
    mart_immo__accessibilite_commune
    ================================
    Table analytique finale : accessibilite immobiliere par commune IDF.

    Croise prix immobiliers, revenus locaux, structure d'age, loyers
    et delinquance pour produire des indicateurs d'accessibilite.

    Grain : commune IDF x annee

    KPI principaux :
    - ratio_prix_m2_revenu_mensuel : mois de revenu pour 1 m2
    - ratio_achat_revenu_annuel : annees de revenu pour un achat median
    - loyer_m2_median : loyer predit au m2 (fixe, carte des loyers 2025)
    - taux_delinquance : faits pour 1000 habitants (variable par annee)

    Limites :
    - Filosofi, RP et loyers sont fixes ; seuls prix et delinquance varient.
    - L'indice de tension est un classement relatif, pas une mesure absolue.
*/

with prix as (
    select * from {{ ref('int_immo__prix_commune_annee') }}
),

revenus as (
    select * from {{ ref('int_revenus__commune') }}
),

demographie as (
    select * from {{ ref('int_demo__structure_age_commune') }}
),

geo as (
    select * from {{ ref('int_geo__communes_idf') }}
),

delinquance as (
    select * from {{ ref('stg_logement__delinquance_communes') }}
),

joined as (
    select
        -- Geographie
        g.code_commune,
        r.nom_commune,
        g.code_departement,
        g.zone_idf,

        -- Annee (vient des prix)
        p.annee,

        -- Population et demographie (fixe RP 2021)
        r.population_2021,
        d.population_totale as population_rp,
        d.pop_25_39,
        d.part_25_39,
        d.part_60_plus,

        -- Revenus (fixe Filosofi 2021)
        r.niveau_vie_median,
        r.niveau_vie_q1,
        r.niveau_vie_q3,
        r.taux_pauvrete_60,
        r.indice_gini,
        r.ratio_interdecile_d9_d1,
        r.nb_menages_fiscaux,

        -- Loyers (fixe, carte des loyers 2025)
        r.loyer_m2_median,

        -- Prix immobiliers (variable par annee)
        p.nb_ventes,
        p.nb_ventes_appartements,
        p.nb_ventes_maisons,
        p.prix_median,
        p.prix_moyen,
        p.prix_m2_median,
        p.prix_m2_moyen,
        p.surface_mediane,

        -- Delinquance (variable par annee)
        del.taux_delinquance_pour_mille,
        del.nb_faits_total as nb_faits_delinquance,

        -- KPI : ratios d'accessibilite

        -- Mois de revenu pour 1 m2
        round(
            p.prix_m2_median / nullif(r.niveau_vie_median / 12.0, 0), 1
        ) as ratio_prix_m2_revenu_mensuel,

        -- Annees de revenu pour un achat median
        round(
            p.prix_median / nullif(r.niveau_vie_median, 0), 1
        ) as ratio_achat_revenu_annuel,

        -- Annees de revenu Q1 pour un achat median
        round(
            p.prix_median / nullif(r.niveau_vie_q1, 0), 1
        ) as ratio_achat_revenu_q1,

        -- Annees de loyer pour un achat (prix median / loyer annuel)
        round(
            p.prix_m2_median / nullif(r.loyer_m2_median * 12, 0), 1
        ) as ratio_achat_loyer_annuel

    from prix p
    inner join geo g on p.code_commune = g.code_commune
    left join revenus r on g.code_commune = r.code_commune
    left join demographie d on g.code_commune = d.code_commune
    left join delinquance del
        on g.code_commune = del.code_commune and p.annee = del.annee
    where
        p.nb_ventes >= {{ var('communes_nb_ventes_min') }}
        and r.niveau_vie_median is not null
)

select
    code_commune, nom_commune, code_departement, zone_idf,
    annee,
    population_2021, population_rp, pop_25_39, part_25_39, part_60_plus,
    niveau_vie_median, niveau_vie_q1, niveau_vie_q3,
    taux_pauvrete_60, indice_gini, ratio_interdecile_d9_d1, nb_menages_fiscaux,
    loyer_m2_median,
    nb_ventes, nb_ventes_appartements, nb_ventes_maisons,
    prix_median, prix_moyen, prix_m2_median, prix_m2_moyen, surface_mediane,
    taux_delinquance_pour_mille, nb_faits_delinquance,
    ratio_prix_m2_revenu_mensuel, ratio_achat_revenu_annuel,
    ratio_achat_revenu_q1, ratio_achat_loyer_annuel
from joined
