import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_date

def process_supply_data():
    """
    Performs data cleansing and transformation for the Supply dataset.
    Implements advanced Data Quality (DQ) handling to ensure pipeline resilience 
    against anomalous negative values while preserving historical integrity.
    """
    spark = SparkSession.builder \
        .appName("Clean_Supply_Estimates") \
        .master("local[*]") \
        .getOrCreate()

    try:
        print("[INFO] Starting Silver Layer processing for Supply data")
        
        # 1. Ingest Bronze Data
        # Using multiline option for nested EIA API JSON structure
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
        
        # 3. Initial Filtering
        # Removes null values and targets specific petroleum products
        silver_df = silver_df.filter(
            col("volume_k_barrels").isNotNull() &
            (col("product_name").contains("Crude Oil") | col("product_name").contains("Distillate Fuel Oil"))
        )

        # 4. Data Quality (DQ) Check and Resilience Handling
        print("[INFO] Executing Data Quality validation")
        
        total_count = silver_df.count()
        if total_count == 0:
            raise ValueError("DQ Critical Error: Supply dataframe is empty after initial filtering")

        # Handle negative volumes: Filter out anomalies instead of stopping the pipeline
        negative_records = silver_df.filter(col("volume_k_barrels") < 0)
        negative_count = negative_records.count()
        
        if negative_count > 0:
            print(f"[WARN] Data Quality Issue: Detected {negative_count} records with negative volumes.")
            print("[INFO] Action: Filtering out anomalous negative records to preserve Data Warehouse integrity.")
            
            # Retention of strictly positive or zero values
            silver_df = silver_df.filter(col("volume_k_barrels") >= 0)
        else:
            print("[INFO] DQ Check Passed: No negative values detected.")

        # 5. Persistence to Silver Layer (Parquet)
        output_path = "/opt/airflow/datalake/silver/supply_estimates"
        silver_df.write.mode("overwrite").parquet(output_path)
        
        print(f"[INFO] Successfully persisted cleaned supply data to {output_path}")

    except Exception as e:
        print(f"[ERROR] Silver Layer pipeline failed: {str(e)}")
        raise e
    finally:
        # Resource cleanup
        spark.stop()

if __name__ == "__main__":
    process_supply_data()