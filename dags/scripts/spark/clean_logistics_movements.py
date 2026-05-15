import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_date, concat, lit

# Add current directory to path so we can import our helper function
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from dq_helpers import apply_quarantine

def process_movements_data():
    spark = SparkSession.builder \
        .appName("Clean_Logistics_Movements") \
        .master("local[*]") \
        .getOrCreate()

    try:
        print("[INFO] Starting Silver Layer processing for Logistics Movements data")
        
        # 1. Ingest Bronze Data
        raw_df = spark.read.option("multiline", "true").json("/opt/airflow/datalake/bronze/movements_*.json")
        exploded_df = raw_df.select(explode(col("response.data")).alias("data"))
        
        # Construct exact date from YYYY-MM
        date_col = to_date(concat(col("data.period"), lit("-01")), "yyyy-MM-dd")

        # 2. Transform Schema and Cast Data Types
        silver_df = exploded_df.select(
            date_col.alias("month_date"),
            col("data.value").cast("double").alias("volume_k_barrels_per_day"),
            col("data.product-name").alias("product_name"),
            col("data.area-name").alias("country_code"),
            col("data.series-description").alias("description")
        )

        # 3. Data Quality (DQ) Checks
        print("--- Running Data Quality Checks ---")
        
        # Dataset-level check: Hard fail if empty
        row_count = silver_df.count()
        if row_count == 0:
            raise ValueError("DQ Critical Error: Logistics movements dataframe is empty!")

        # Row-level check: Apply Quarantine logic using helper function
        valid_condition = col("volume_k_barrels_per_day").isNotNull()
        quarantine_path = "/opt/airflow/datalake/quarantine/logistics_movements"
        
        valid_df = apply_quarantine(
            df=silver_df,
            valid_condition=valid_condition,
            error_reason_text="Null Volume Detected",
            quarantine_path=quarantine_path
        )

        if valid_df.count() == 0:
            raise ValueError("DQ Critical Error: No valid records remaining after quarantine.")

        # 4. Persistence to Silver Layer
        output_path = "/opt/airflow/datalake/silver/logistics_movements"
        valid_df.write.mode("overwrite").parquet(output_path)
        print(f"[INFO] Successfully processed movements data to {output_path}")

    except Exception as e:
        print(f"[ERROR] Pipeline Failed: {e}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    process_movements_data()