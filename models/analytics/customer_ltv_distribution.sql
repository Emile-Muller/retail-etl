{{ config(materialized='table') }}

WITH customer_ltv AS (
    SELECT
        customer_id,
        SUM(total_sale) AS lifetime_value
    FROM {{ ref('sales') }}
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id
)

SELECT
    CASE
        WHEN lifetime_value BETWEEN 0 AND 100 THEN '£0–100'
        WHEN lifetime_value > 100 AND lifetime_value <= 500 THEN '£100–500'
        WHEN lifetime_value > 500 AND lifetime_value <= 1000 THEN '£500–1000'
        ELSE '£1000+'
    END AS ltv_bucket,
    CASE
        WHEN lifetime_value BETWEEN 0 AND 100 THEN 1
        WHEN lifetime_value > 100 AND lifetime_value <= 500 THEN 2
        WHEN lifetime_value > 500 AND lifetime_value <= 1000 THEN 3
        ELSE 4
    END AS sort_order,
    COUNT(*) AS num_customers,
    ROUND(COUNT(*) / SUM(COUNT(*)) OVER (), 5) AS pct_customers
FROM customer_ltv
GROUP BY ltv_bucket, sort_order
