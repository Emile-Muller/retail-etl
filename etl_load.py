import os
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector

def load_csv_to_snowflake():
	"""
	Load a CSV file into a Snowflake table via a staging process
	"""
	filename = "online_retail_II.csv"
	
	load_dotenv()
	
	try:
		print("Connecting to Snowflake...")	
		ctx = snowflake.connector.connect(
			user=os.getenv("SNOWFLAKE_USER"),
			password=os.getenv("SNOWFLAKE_PASSWORD"),
			account=os.getenv("SNOWFLAKE_ACCOUNT"),
			warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
			database=os.getenv("SNOWFLAKE_DATABASE"),
			schema=os.getenv("SNOWFLAKE_SCHEMA")
		)
		cs = ctx.cursor()
		print("Connected to Snowflake.")

		print("Creating raw table...")
		cs.execute("""
			CREATE TABLE IF NOT EXISTS raw_online_retail (
				invoice STRING,
				stock_code STRING,
				description STRING,
				quantity NUMBER,
				invoice_date TIMESTAMP,
				price NUMBER(10,2),
				customer_id STRING,
				country STRING
			)
		""")
		print("Created raw table.")
		
		# Removing existing content if there is any
		print("Truncating table content...")
		cs.execute("TRUNCATE TABLE IF EXISTS raw_online_retail")
		print("Truncated table content.")

		print("Loading CSV into raw table...")
		cs.execute("CREATE STAGE IF NOT EXISTS stg_online_retail")
		
		csv_path = Path(__file__).parent / "data" / filename
		csv_uri = f"file://{csv_path.as_posix()}"
		put_cmd = f"PUT '{csv_uri}' @stg_online_retail OVERWRITE=TRUE"
		cs.execute(put_cmd)

		cs.execute(f"""
			COPY INTO raw_online_retail
			FROM @stg_online_retail/{filename}
			FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
		""")
		print("CSV loaded into raw table.")
		
		# Removing the ".0" from customer_id which doesn't seem intentional
		print("Truncating customer_id to 5 characters...")
		cs.execute("""
			UPDATE raw_online_retail
			SET customer_id = LEFT(
				TRIM(REGEXP_REPLACE(CAST(customer_id AS STRING), '\\.0$', '')),
				5
			)
		""")
		print("Truncated customer_id to 5 characters.")
	
	except Exception as e:
		print("Connection or query failed:", e)
		
	finally:
		try:
			cs.close()
			ctx.close()
		except:
			pass

def main():
	load_csv_to_snowflake()

if __name__ == "__main__":
	main()