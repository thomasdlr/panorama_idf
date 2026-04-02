/*
    Vérifie que les prix au m² médians sont toujours positifs dans le mart final.
    Un prix négatif ou nul indiquerait un problème de données ou de filtrage.
*/

select
    code_commune,
    annee,
    prix_m2_median

from {{ ref('mart_immo__accessibilite_commune') }}
where prix_m2_median <= 0
