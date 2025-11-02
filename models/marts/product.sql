{{ config(materialized='table') }}

WITH source AS (
	SELECT DISTINCT
		UPPER(stock_code) AS product_id,
		TRIM(REGEXP_REPLACE(description, '[[:space:]]+', ' ')) AS product_name
	FROM raw_online_retail
	WHERE
        stock_code IS NOT NULL
        AND description IS NOT NULL
),

deduped AS (
    SELECT
        product_id,
		CONCAT(
			UPPER(SUBSTR(product_name, 1, 1)),
			LOWER(SUBSTR(product_name, 2))
		) AS product_name,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY LENGTH(product_name) DESC
        ) AS rn
    FROM source
)

SELECT 
    product_id,
    product_name
FROM deduped
WHERE rn = 1