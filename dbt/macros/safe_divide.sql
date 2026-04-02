/*
    Division sûre : retourne null au lieu d'erreur sur division par zéro.
*/

{% macro safe_divide(numerator, denominator) %}
    ({{ numerator }}) / nullif({{ denominator }}, 0)
{% endmacro %}
