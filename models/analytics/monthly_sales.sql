{{ config(materialized='table') }}

WITH monthly_sales_base AS (
    SELECT
        DATE_TRUNC('month', td.transaction_date) AS month_start,
        s.customer_id
    FROM sales AS s
    JOIN transaction_date AS td
        ON s.transaction_id = td.transaction_id
    WHERE s.customer_id IS NOT NULL
    GROUP BY 1, 2
),

customer_window_3m AS (
    SELECT
        a.month_start AS month_start,
        b.customer_id,
        COUNT(DISTINCT b.month_start) AS repeated_month_purchases
    FROM monthly_sales_base a
    JOIN monthly_sales_base b
        ON b.month_start BETWEEN DATEADD(month, -2, a.month_start) AND a.month_start
    GROUP BY a.month_start, b.customer_id
),

active_customers_3m AS (
    SELECT
        month_start,
        COUNT(DISTINCT customer_id) AS active_customers_3m,
        COUNT(DISTINCT CASE WHEN repeated_month_purchases > 1 THEN customer_id END) AS repeat_customers_3m
    FROM customer_window_3m
    GROUP BY month_start
)

SELECT
	TO_CHAR(td.transaction_date, 'YYYY-MM') AS year_month,
    td.year,
    td.month,
    a.active_customers_3m,
    a.repeat_customers_3m,
    ROUND(a.repeat_customers_3m / NULLIF(a.active_customers_3m, 0), 5) AS customer_repeat_rate,
	COUNT(DISTINCT s.customer_id) AS active_customers_1m,
    SUM(s.total_sale) AS total_sales,
    COUNT(DISTINCT s.transaction_id) AS transaction_count,
    SUM(s.total_sale) / COUNT(DISTINCT s.transaction_id) AS avg_order_value
FROM {{ ref('sales') }} AS s
JOIN {{ ref('transaction_date') }} AS td
    ON s.transaction_id = td.transaction_id
JOIN active_customers_3m AS a
    ON DATE_TRUNC('month', td.transaction_date) = a.month_start
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1