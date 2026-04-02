/*
    Vérifie que la part des 25-39 ans est un ratio valide (entre 0 et 1).
*/

select
    code_commune,
    part_25_39

from {{ ref('int_demo__structure_age_commune') }}
where
    part_25_39 < 0
    or part_25_39 > 1
