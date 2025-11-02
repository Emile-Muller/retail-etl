{{ config(materialized='table') }}

SELECT
    c.country,
    COUNT(DISTINCT s.customer_id) AS customers,
    SUM(total_sale) AS total_sales,
    COUNT(DISTINCT s.transaction_id) AS transaction_count,
    SUM(total_sale) / COUNT(DISTINCT s.transaction_id) AS avg_order_value
FROM {{ ref('sales') }} AS s
JOIN {{ ref('customer') }} c
    ON s.customer_id = c.customer_id
GROUP BY country
ORDER BY customers DESC