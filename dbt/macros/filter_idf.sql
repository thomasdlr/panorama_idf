/*
    Macro utilitaire : filtre sur les départements Île-de-France.
    Utilisable dans n'importe quel modèle via {{ filter_idf('code_departement') }}.
*/

{% macro filter_idf(column_name) %}
    {{ column_name }} in ('75', '77', '78', '91', '92', '93', '94', '95')
{% endmacro %}
