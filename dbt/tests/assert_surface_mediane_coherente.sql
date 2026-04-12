/*
    Vérifie que les surfaces médianes sont dans une plage réaliste (5-500 m²).
*/

select code_commune, annee, surface_mediane
from {{ ref('mart_immo__accessibilite_commune') }}
where surface_mediane < 5 or surface_mediane > 500
