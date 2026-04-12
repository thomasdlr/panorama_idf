/*
    Vérifie que les loyers au m² sont positifs quand ils existent.
*/

select code_commune, loyer_m2_median
from {{ ref('mart_immo__accessibilite_commune') }}
where loyer_m2_median is not null and loyer_m2_median <= 0
