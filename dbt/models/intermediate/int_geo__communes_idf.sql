/*
    int_geo__communes_idf
    ---------------------
    Référentiel des communes d'Île-de-France uniquement.
    Base de jointure pour tous les modèles suivants.

    Grain : commune IDF
*/

select
    code_commune,
    nom_commune,
    code_departement,
    code_region,
    zone_idf,
    type_commune

from {{ ref('stg_insee__cog_communes') }}
where is_idf = true
