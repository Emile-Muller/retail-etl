{{ config(materialized='table') }}

SELECT
    s.product_id,
    p.product_name,
    SUM(total_sale) AS total_sales,
    COUNT(DISTINCT s.transaction_id) AS transaction_count,
	AVG(unit_price) AS avg_unit_price,
	AVG(quantity) AS avg_quantity,
    SUM(total_sale) / COUNT(DISTINCT s.transaction_id) AS avg_order_value,
    ROW_NUMBER() OVER (ORDER BY SUM(total_sale) DESC) AS product_rank
FROM {{ ref('sales') }} AS s
JOIN {{ ref('product') }} AS p
    ON s.product_id = p.product_id
WHERE p.product_id NOT IN ('DOT', 'POST')
GROUP BY s.product_id, p.product_name
ORDER BY total_sales DESC
LIMIT 100