/*
    Vérifie que le ratio achat/revenu annuel reste dans une plage plausible
    pour les communes avec un volume de ventes significatif (>= 10).
    En IDF, on s'attend à des ratios entre 1 et 150 années.
    Au-delà, c'est probablement un artefact de données (peu de ventes,
    prix atypiques, ou revenus très bas).
*/

select
    code_commune,
    nom_commune,
    annee,
    ratio_achat_revenu_annuel,
    nb_ventes

from {{ ref('mart_immo__accessibilite_commune') }}
where
    nb_ventes >= 10
    and (ratio_achat_revenu_annuel < 1
         or ratio_achat_revenu_annuel > 150)
