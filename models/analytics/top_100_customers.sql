{{ config(materialized='table') }}

SELECT
    s.customer_id,
    c.country,
    SUM(total_sale) AS clv,
    COUNT(DISTINCT s.transaction_id) AS transaction_count,
    SUM(total_sale) / COUNT(DISTINCT s.transaction_id) AS avg_order_value,
    MIN(td.transaction_date) AS first_purchase,
    MAX(td.transaction_date) AS last_purchase,
    ROW_NUMBER() OVER (ORDER BY SUM(total_sale) DESC) AS customer_rank
FROM {{ ref('sales') }} AS s
JOIN {{ ref('customer') }} AS c
    ON s.customer_id = c.customer_id
JOIN {{ ref('transaction_date') }} td
    ON s.transaction_id = td.transaction_id
WHERE s.customer_id IS NOT NULL
GROUP BY s.customer_id, c.country
ORDER BY clv DESC
LIMIT 100