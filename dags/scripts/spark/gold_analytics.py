import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, year, month, quarter, weekofyear, dayofweek
from pyspark.sql.functions import sum as _sum, max as _max

def build_gold_star_schema():
    """
    Orchestrates the transformation of silver layer data into a curated gold layer 
    using a Star Schema architecture. 
    
    Key Features:
    - Implements safe-save mechanisms to bypass OS/Docker file system locks.
    - Applies strict aggregations to guarantee data integrity.
    - Preserves Foreign Keys (Product, Area) in the Fact table for comprehensive modeling.
    """
    spark = SparkSession.builder \
        .appName("Gold_Star_Schema_Transformation") \
        .master("local[*]") \
        .getOrCreate()

    try:
        print("[INFO] Starting Full Star Schema Transformation")
        
        # 1. Load data from the Silver Layer
        prices_df = spark.read.parquet("/opt/airflow/datalake/silver/petroleum_prices")
        supply_df = spark.read.parquet("/opt/airflow/datalake/silver/supply_estimates")

        # ==========================================
        # DIMENSION TABLES GENERATION
        # ==========================================
        
        # dim_date: Temporal attributes
        dates_prices = prices_df.select("date")
        dates_supply = supply_df.select("date")
        dim_date = dates_prices.union(dates_supply).distinct() \
            .withColumn("year", year(col("date"))) \
            .withColumn("month", month(col("date"))) \
            .withColumn("quarter", quarter(col("date"))) \
            .withColumn("week_of_year", weekofyear(col("date"))) \
            .withColumn("day_of_week", dayofweek(col("date")))

        # dim_product: Product master data
        products_prices = prices_df.select("product_name").distinct()
        products_supply = supply_df.select("product_name").distinct()
        dim_product = products_prices.union(products_supply).distinct()

        # dim_area: Geographic master data
        dim_area = supply_df.select("area").distinct()

        # ==========================================
        # FACT TABLE GENERATION
        # ==========================================
        
        # Step 1: Prepare Prices (Targeting No 2 Diesel)
        diesel_prices = prices_df.filter(col("product_name").contains("No 2 Diesel")) \
            .withColumn("join_year", year(col("date"))) \
            .withColumn("join_week", weekofyear(col("date"))) \
            .select(
                col("date").alias("price_date"), 
                col("product_name").alias("price_product"), # Retained as Foreign Key
                "price_usd_per_gal", 
                "join_year", "join_week"
            )

        # Step 2: Prepare Supply (Aggregating Distillate Fuel Oil stocks)
        diesel_supply = supply_df.filter(
            (col("process_name") == "Ending Stocks") &
            (col("area") == "U.S.") &
            (col("product_name").contains("Distillate Fuel Oil"))
        ).withColumn("join_year", year(col("date"))) \
         .withColumn("join_week", weekofyear(col("date"))) \
         .groupBy("join_year", "join_week", "area") \
         .agg(
             _sum("volume_k_barrels").alias("total_stock_k_barrels")
         )

        # Step 3: Consolidate Fact Table via Temporal Keys
        # Preserving date, product_name, and area to form a complete Star Schema
        fact_diesel_market = diesel_prices.join(
            diesel_supply, 
            (diesel_prices.join_year == diesel_supply.join_year) & 
            (diesel_prices.join_week == diesel_supply.join_week), 
            how="inner"
        ).select(
            col("price_date").alias("date"), 
            col("price_product").alias("product_name"), 
            col("area"),
            round(col("price_usd_per_gal"), 3).alias("price_usd_per_gal"),
            col("total_stock_k_barrels")
        )

        # ==========================================
        # DATA PERSISTENCE
        # ==========================================
        base_gold_path = "/opt/airflow/datalake/gold"
        
        def safe_save_parquet(df, folder_name):
            target_path = f"{base_gold_path}/{folder_name}"
            # Forcefully clear target directory to prevent 'cannotClearOutputDirectoryError'
            if os.path.exists(target_path):
                shutil.rmtree(target_path)
            
            df.write.mode("overwrite").parquet(target_path)
            print(f"[INFO] Successfully saved {folder_name} to Gold Layer.")

        safe_save_parquet(dim_date, "dim_date")
        safe_save_parquet(dim_product, "dim_product")
        safe_save_parquet(dim_area, "dim_area")
        safe_save_parquet(fact_diesel_market, "fact_diesel_market")
        
        print(f"[INFO] Gold Star Schema pipeline completed successfully.")

    except Exception as e:
        print(f"[ERROR] Critical failure in Gold Star Schema Pipeline: {e}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    build_gold_star_schema()