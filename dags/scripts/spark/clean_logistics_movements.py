from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_date, concat, lit

def process_movements_data():
    spark = SparkSession.builder \
        .appName("Clean_Logistics_Movements") \
        .master("local[*]") \
        .getOrCreate()

    try:
        raw_df = spark.read.option("multiline", "true").json("/opt/airflow/datalake/bronze/movements_*.json")
        exploded_df = raw_df.select(explode(col("response.data")).alias("data"))
        
        date_col = to_date(concat(col("data.period"), lit("-01")), "yyyy-MM-dd")

        silver_df = exploded_df.select(
            date_col.alias("month_date"),
            col("data.value").cast("double").alias("volume_k_barrels_per_day"),
            col("data.product-name").alias("product_name"),
            col("data.area-name").alias("country_code"),
            col("data.series-description").alias("description")
        )

        # DATA QUALITY CHECKS
        print("--- Running Data Quality Checks ---")
        
        row_count = silver_df.count()
        if row_count == 0:
            raise ValueError("DQ Error: Logistics movements dataframe is empty!")

        null_volumes = silver_df.filter(col("volume_k_barrels_per_day").isNull()).count()
        if null_volumes > 0:
            raise ValueError(f"DQ Error: Found {null_volumes} null volume records!")
            
        print("DQ Checks Passed")

        output_path = "/opt/airflow/datalake/silver/logistics_movements"
        silver_df.write.mode("overwrite").parquet(output_path)
        print(f"Successfully processed movements data to {output_path}")

    except Exception as e:
        print(f"Pipeline Failed: {e}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    process_movements_data()