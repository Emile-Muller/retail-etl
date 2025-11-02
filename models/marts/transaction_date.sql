{{ config(materialized='table') }}

WITH source AS (
    SELECT DISTINCT
        invoice AS transaction_id,
        invoice_date AS transaction_ts
    FROM raw_online_retail
    WHERE
        invoice IS NOT NULL
        AND invoice_date IS NOT NULL
),

deduped AS (
    SELECT
        transaction_id,
        transaction_ts,
		ROW_NUMBER() OVER (
			PARTITION BY transaction_id
			ORDER BY transaction_ts DESC
		) AS rn
    FROM source    
)

SELECT 
    transaction_id,
    transaction_ts,
	CAST(transaction_ts AS DATE) AS transaction_date,
	TO_CHAR(transaction_ts, 'YYYY') AS year,
	TO_CHAR(transaction_ts, 'MM') AS month,
	TO_CHAR(transaction_ts, 'MMMM') AS month_name,
	TO_CHAR(transaction_ts, 'DD') AS day,
	DAYOFWEEK(transaction_ts) AS day_of_week,
	TO_CHAR(transaction_ts, 'DY') AS day_name,
	CASE WHEN DAYOFWEEK(transaction_ts) IN (0,6) THEN TRUE ELSE FALSE END AS is_weekend,
	QUARTER(transaction_ts) AS quarter,
	WEEK(transaction_ts) AS week_of_year,
	TO_CHAR(transaction_ts, 'HH24') AS hour,
	TO_CHAR(transaction_ts, 'MI') AS minute
FROM deduped
WHERE rn = 1