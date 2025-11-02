{{ config(materialized='table') }}

SELECT
    t.transaction_id AS transaction_id,
    r.customer_id,
    p.product_id AS product_id,
    r.quantity,
	CAST(r.price AS NUMBER(10,2)) AS unit_price,
    CAST(r.quantity * r.price AS NUMBER(10,2)) AS total_sale
FROM raw_online_retail AS r
JOIN {{ ref('transaction_date') }} AS t
	ON r.invoice = t.transaction_id
JOIN {{ ref('product') }} AS p
	ON UPPER(r.stock_code) = p.product_id
LEFT JOIN {{ ref('customer') }} AS c
	ON r.customer_id = c.customer_id
WHERE
	r.invoice IS NOT NULL
	AND r.stock_code IS NOT NULL
	AND r.quantity IS NOT NULL
	AND r.quantity != 0
	AND r.price IS NOT NULL
	AND r.price > 0