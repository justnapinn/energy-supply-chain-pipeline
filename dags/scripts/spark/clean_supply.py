from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_date

def process_supply_data():
    spark = SparkSession.builder \
        .appName("Clean_Supply_Estimates") \
        .master("local[*]") \
        .getOrCreate()

    try:
        raw_df = spark.read.option("multiline", "true").json("/opt/airflow/datalake/bronze/supply_*.json")
        exploded_df = raw_df.select(explode(col("response.data")).alias("data"))
        
        silver_df = exploded_df.select(
            to_date(col("data.period"), "yyyy-MM-dd").alias("date"),
            col("data.value").cast("double").alias("volume_k_barrels"),
            col("data.product-name").alias("product_name"),
            col("data.process-name").alias("process_name"),
            col("data.area-name").alias("area")
        )
        
        silver_df = silver_df.filter(
            col("volume_k_barrels").isNotNull() &
            (col("product_name").contains("Crude Oil") | col("product_name").contains("Distillate Fuel Oil"))
        )

        # DATA QUALITY CHECKS
        print("--- Running Data Quality Checks ---")
        
        row_count = silver_df.count()
        if row_count == 0:
            raise ValueError("DQ Error: Supply dataframe is empty after filtering!")

        # Check if there are any negative values (inventory/production should not be negative)
        negative_volumes = silver_df.filter(col("volume_k_barrels") < 0).count()
        if negative_volumes > 0:
            raise ValueError(f"DQ Error: Found {negative_volumes} records with negative volumes!")
            
        print("DQ Checks Passed")

        output_path = "/opt/airflow/datalake/silver/supply_estimates"
        silver_df.write.mode("overwrite").parquet(output_path)
        print(f"Successfully processed supply data to {output_path}")

    except Exception as e:
        print(f"Pipeline Failed: {e}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    process_supply_data()