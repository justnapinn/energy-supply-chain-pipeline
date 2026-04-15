from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_date

def process_prices_data():
    spark = SparkSession.builder \
        .appName("Clean_Petroleum_Prices") \
        .master("local[*]") \
        .getOrCreate()

    try:
        raw_df = spark.read.option("multiline", "true").json("/opt/airflow/datalake/bronze/prices_*.json")
        exploded_df = raw_df.select(explode(col("response.data")).alias("data"))
        
        silver_df = exploded_df.select(
            to_date(col("data.period"), "yyyy-MM-dd").alias("date"),
            col("data.value").cast("double").alias("price_usd_per_gal"),
            col("data.product-name").alias("product_name"),
            col("data.process-name").alias("process_name")
        )

        # DATA QUALITY CHECKS
        print("--- Running Data Quality Checks ---")
        
        # 1. Check if data exists (Completeness)
        row_count = silver_df.count()
        if row_count == 0:
            raise ValueError("DQ Error: No data found in the prices dataframe!")

        # 2. Check if prices are not null and positive (Validity)
        invalid_prices = silver_df.filter((col("price_usd_per_gal").isNull()) | (col("price_usd_per_gal") <= 0)).count()
        if invalid_prices > 0:
            raise ValueError(f"DQ Error: Found {invalid_prices} invalid or null price records!")
            
        print("DQ Checks Passed")

        # Save as Parquet
        output_path = "/opt/airflow/datalake/silver/petroleum_prices"
        silver_df.write.mode("overwrite").parquet(output_path)
        print(f"Successfully processed prices data to {output_path}")

    except Exception as e:
        print(f"Pipeline Failed: {e}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    process_prices_data()