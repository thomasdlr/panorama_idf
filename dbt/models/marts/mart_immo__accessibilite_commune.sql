/*
    mart_immo__accessibilite_commune
    ================================
    Table analytique finale : accessibilité immobilière par commune IDF.

    Croise prix immobiliers, revenus locaux et structure d'âge
    pour produire des indicateurs de "tension d'accès" à l'achat.

    Grain : commune IDF × année

    KPI principaux :
    - ratio_prix_m2_revenu : prix médian au m² / revenu disponible médian mensuel
      → combien de mois de revenu pour 1 m²
    - ratio_achat_revenu : prix médian transaction / revenu annuel médian
      → combien d'années de revenu pour un achat médian
    - indice_tension : score composite normalisé (voir calcul ci-dessous)

    Limites méthodologiques :
    - Le revenu utilisé est le niveau de vie Filosofi (par UC), pas le revenu
      des ménages acheteurs.
    - Filosofi 2021 est fixe ; les prix varient par année. Le ratio
      prix/revenu est donc un proxy, pas une mesure exacte du taux d'effort.
    - La structure d'âge (RP 2021) est également fixe dans le temps.
    - L'indice de tension est un outil de classement relatif, pas une mesure
      absolue de difficulté d'accès.
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

joined as (
    select
        -- Géographie
        g.code_commune,
        r.nom_commune,
        g.code_departement,
        g.zone_idf,

        -- Année (vient des prix)
        p.annee,

        -- Population et démographie (fixe RP 2021)
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

        -- Prix immobiliers (variable par année)
        p.nb_ventes,
        p.nb_ventes_appartements,
        p.nb_ventes_maisons,
        p.prix_median,
        p.prix_moyen,
        p.prix_m2_median,
        p.prix_m2_moyen,
        p.surface_mediane,

        -- ═══════════════════════════════════════
        -- KPI : ratios d'accessibilité
        -- ═══════════════════════════════════════

        -- Ratio prix m² / revenu mensuel médian
        -- "Combien de mois de niveau de vie médian pour 1 m²"
        round(
            p.prix_m2_median / nullif(r.niveau_vie_median / 12.0, 0),
            1
        ) as ratio_prix_m2_revenu_mensuel,

        -- Ratio prix médian transaction / revenu annuel
        -- "Combien d'années de niveau de vie pour un achat médian"
        round(
            p.prix_median / nullif(r.niveau_vie_median, 0),
            1
        ) as ratio_achat_revenu_annuel,

        -- Ratio prix médian / revenu Q1 (ménages modestes)
        -- Plus sévère : accessibilité pour le quart le moins aisé
        round(
            p.prix_median / nullif(r.niveau_vie_q1, 0),
            1
        ) as ratio_achat_revenu_q1

    from prix p
    inner join geo g on p.code_commune = g.code_commune
    left join revenus r on g.code_commune = r.code_commune
    left join demographie d on g.code_commune = d.code_commune
    where
        -- On exclut les communes avec trop peu de ventes (bruit statistique)
        p.nb_ventes >= 5
        -- On exclut celles sans revenu connu
        and r.niveau_vie_median is not null
)

select * from joined
