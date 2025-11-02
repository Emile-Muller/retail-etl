{{ config(materialized='table') }}

WITH customer_orders AS (
    SELECT
        customer_id,
        COUNT(DISTINCT transaction_id) AS num_orders
    FROM {{ ref('sales') }}
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id
)

SELECT
    CASE
        WHEN num_orders = 1 THEN '1 order'
        WHEN num_orders BETWEEN 2 AND 3 THEN '2–3 orders'
        WHEN num_orders BETWEEN 4 AND 5 THEN '4–5 orders'
        ELSE '6+ orders'
    END AS order_bucket,
    COUNT(*) AS num_customers,
    ROUND(COUNT(*) / SUM(COUNT(*)) OVER (), 5) AS pct_customers
FROM customer_orders
GROUP BY order_bucket

