/*
    Vérifie que le ratio achat/revenu annuel reste dans une plage plausible.
    En IDF, on s'attend à des ratios entre 1 et 100 années.
    Au-delà, c'est probablement un artefact de données.
*/

select
    code_commune,
    nom_commune,
    annee,
    ratio_achat_revenu_annuel

from {{ ref('mart_immo__accessibilite_commune') }}
where
    ratio_achat_revenu_annuel < 1
    or ratio_achat_revenu_annuel > 100
