/*
    Macro utilitaire pour caster les colonnes Filosofi.
    Les données INSEE utilisent souvent la virgule comme séparateur décimal
    et des chaînes vides ou 's' (secret statistique) comme valeurs manquantes.
*/

{% macro cast_filosofi_numeric(column_name) %}
    cast(
        nullif(
            nullif(
                replace(trim({{ column_name }}), ',', '.'),
                ''
            ),
            's'
        ) as double
    )
{% endmacro %}
