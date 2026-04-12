/*
    Vérifie que les parts d'âge sont des ratios valides (entre 0 et 1)
    et que leur somme ne dépasse pas 1.
*/

select code_commune, part_25_39, part_60_plus
from {{ ref('int_demo__structure_age_commune') }}
where
    part_60_plus < 0 or part_60_plus > 1
    or (part_25_39 + part_60_plus) > 1.01
