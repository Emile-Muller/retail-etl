{% test length_equals(model, column_name, value) %}

SELECT *
FROM {{ model }}
WHERE LENGTH({{ column_name }}) != {{ value }}

{% endtest %}
