/*
    Vérifie que les taux de délinquance sont positifs quand ils existent.
*/

select code_commune, annee, taux_delinquance_pour_mille
from {{ ref('mart_immo__accessibilite_commune') }}
where taux_delinquance_pour_mille is not null and taux_delinquance_pour_mille < 0
