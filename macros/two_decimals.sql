{% test two_decimals(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE ROUND({{ column_name }}, 2) != {{ column_name }}

{% endtest %}
