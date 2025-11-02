{{ config(materialized='table') }}

WITH source AS (
	SELECT DISTINCT
		customer_id,
		country,
		invoice_date
	FROM raw_online_retail
	WHERE
		customer_id IS NOT NULL
),

deduped AS (
	SELECT
		customer_id,
		country,
		ROW_NUMBER() OVER (
			PARTITION BY customer_id
			ORDER BY invoice_date DESC
		) AS rn
	FROM source
)

SELECT
	customer_id,
	country
FROM deduped
WHERE rn = 1