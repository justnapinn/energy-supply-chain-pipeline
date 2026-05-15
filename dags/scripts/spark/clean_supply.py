import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_date

# Add the current directory to the system path to import the helper function
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from dq_helpers import apply_quarantine

def process_supply_data():
    """
    Performs data cleansing and transformation for the Supply dataset.
    Filters for 'Ending Stocks' and implements a Quarantine Zone 
    for negative values to ensure data observability and auditability.
    """
    spark = SparkSession.builder \
        .appName("Clean_Supply_Estimates") \
        .master("local[*]") \
        .getOrCreate()

    try:
        print("[INFO] Starting Silver Layer processing for Supply data")
        
        # 1. Ingest Bronze Data
        raw_df = spark.read.option("multiline", "true").json("/opt/airflow/datalake/bronze/supply_*.json")
        exploded_df = raw_df.select(explode(col("response.data")).alias("data"))
        
        # 2. Transform Schema and Cast Data Types
        silver_df = exploded_df.select(
            to_date(col("data.period"), "yyyy-MM-dd").alias("date"),
            col("data.value").cast("double").alias("volume_k_barrels"),
            col("data.product-name").alias("product_name"),
            col("data.process-name").alias("process_name"),
            col("data.area-name").alias("area")
        )
        
        # 3. Business Logic Filter & Data Quality (DQ) Checks
        print("[INFO] Running Data Quality Checks")
        
        # 3.1 Business Logic: Filter only "Ending Stocks"
        # We do this before DQ checks because other processes (like Net Imports) 
        # can legitimately have negative values based on economic principles.
        silver_df = silver_df.filter(col("process_name") == "Ending Stocks")

        # 3.2 Dataset-level check
        row_count = silver_df.count()
        if row_count == 0:
            raise ValueError("DQ Critical Error: Supply dataframe is empty after filtering Ending Stocks.")

        # 3.3 Row-level check: Ending Stocks must not be negative
        valid_condition = col("volume_k_barrels") >= 0
        quarantine_path = "/opt/airflow/datalake/quarantine/supply_estimates"
        
        valid_df = apply_quarantine(
            df=silver_df,
            valid_condition=valid_condition,
            error_reason_text="Negative Volume in Ending Stocks",
            quarantine_path=quarantine_path
        )

        # Ensure we still have data to process after quarantine
        if valid_df.count() == 0:
            raise ValueError("DQ Critical Error: No valid records remaining after quarantine.")

        # 4. Persistence to Silver Layer
        output_path = "/opt/airflow/datalake/silver/supply_estimates"
        valid_df.write.mode("overwrite").parquet(output_path)
        print(f"[INFO] Successfully persisted clean supply data to {output_path}")

    except Exception as e:
        print(f"[ERROR] Silver Layer pipeline failed: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    process_supply_data()