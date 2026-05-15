import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_date

# Add current directory to path so we can import our helper function
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from dq_helpers import apply_quarantine

def process_prices_data():
    spark = SparkSession.builder \
        .appName("Clean_Petroleum_Prices") \
        .master("local[*]") \
        .getOrCreate()

    try:
        print("[INFO] Starting Silver Layer processing for Prices data")
        
        # 1. Ingest Bronze Data
        raw_df = spark.read.option("multiline", "true").json("/opt/airflow/datalake/bronze/prices_*.json")
        exploded_df = raw_df.select(explode(col("response.data")).alias("data"))
        
        # 2. Transform Schema and Cast Data Types
        silver_df = exploded_df.select(
            to_date(col("data.period"), "yyyy-MM-dd").alias("date"),
            col("data.value").cast("double").alias("price_usd_per_gal"),
            col("data.product-name").alias("product_name"),
            col("data.process-name").alias("process_name")
        )

        # 3. Data Quality (DQ) Checks
        print("--- Running Data Quality Checks ---")
        
        # Dataset-level check: Hard fail if empty
        row_count = silver_df.count()
        if row_count == 0:
            raise ValueError("DQ Critical Error: No data found in the prices dataframe!")

        # Row-level check: Apply Quarantine logic using helper function
        valid_condition = col("price_usd_per_gal").isNotNull() & (col("price_usd_per_gal") > 0)
        quarantine_path = "/opt/airflow/datalake/quarantine/petroleum_prices"
        
        valid_df = apply_quarantine(
            df=silver_df,
            valid_condition=valid_condition,
            error_reason_text="Invalid or Null Price",
            quarantine_path=quarantine_path
        )

        # Ensure we still have data after quarantine
        if valid_df.count() == 0:
            raise ValueError("DQ Critical Error: No valid records remaining after quarantine.")

        # 4. Persistence to Silver Layer
        output_path = "/opt/airflow/datalake/silver/petroleum_prices"
        valid_df.write.mode("overwrite").parquet(output_path)
        print(f"[INFO] Successfully processed prices data to {output_path}")

    except Exception as e:
        print(f"[ERROR] Pipeline Failed: {e}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    process_prices_data()