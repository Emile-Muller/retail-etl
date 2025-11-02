{{ config(materialized='table') }}

SELECT
    CASE
        WHEN unit_price < 1 THEN 'Very Low (<£1)'
        WHEN unit_price >= 1 AND unit_price < 2 THEN 'Low (£1–£2)'
        WHEN unit_price >= 2 AND unit_price < 5 THEN 'Mid-Low (£2–£5)'
        WHEN unit_price >= 5 AND unit_price < 10 THEN 'Mid-High (£5–£10)'
        WHEN unit_price >= 10 AND unit_price < 20 THEN 'High (£10–£20)'
        ELSE 'Very High (>£20)'
    END AS price_tier,
    SUM(quantity * unit_price) AS total_sales,
    COUNT(DISTINCT transaction_id) AS transaction_count
FROM sales s
JOIN product p
    ON s.product_id = p.product_id
GROUP BY price_tier
ORDER BY total_sales DESC