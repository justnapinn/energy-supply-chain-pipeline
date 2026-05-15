import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, year, month, quarter, weekofyear, dayofweek, date_format
from pyspark.sql.functions import sum as _sum, when, crc32, abs

def build_gold_star_schema():
    """
    Orchestrates the transformation of silver layer data into a curated gold layer.
    Implements Enterprise Data Modeling standards by generating deterministic 
    Surrogate Keys (via CRC32 Hashing) and adding descriptive dimension attributes.
    """
    spark = SparkSession.builder \
        .appName("Gold_Star_Schema_Transformation") \
        .master("local[*]") \
        .getOrCreate()

    try:
        print("[INFO] Starting Full Star Schema Transformation")
        
        prices_df = spark.read.parquet("/opt/airflow/datalake/silver/petroleum_prices")
        supply_df = spark.read.parquet("/opt/airflow/datalake/silver/supply_estimates")
        movements_df = spark.read.parquet("/opt/airflow/datalake/silver/logistics_movements")

        # ==========================================
        # DIMENSION TABLES GENERATION
        # ==========================================
        
        # 1. Date Dimension
        dates_prices = prices_df.select("date")
        dates_supply = supply_df.select("date")
        dim_date = dates_prices.union(dates_supply).distinct() \
            .withColumn("year", year(col("date"))) \
            .withColumn("month", month(col("date"))) \
            .withColumn("quarter", quarter(col("date"))) \
            .withColumn("week_of_year", weekofyear(col("date"))) \
            .withColumn("day_of_week", dayofweek(col("date")))

        # 2. Product Dimension (With Surrogate Key and Categorization)
        products_prices = prices_df.select("product_name")
        products_supply = supply_df.select("product_name")
        products_movements = movements_df.select("product_name")
        
        dim_product_raw = products_prices.union(products_supply).union(products_movements).distinct()
        
        dim_product = dim_product_raw \
            .withColumn("product_id", abs(crc32(col("product_name"))).cast("bigint")) \
            .withColumn("product_category", 
                        when(col("product_name").contains("Crude"), "Crude Oil")
                        .when(col("product_name").contains("Diesel") | col("product_name").contains("Distillate"), "Distillate Fuel")
                        .when(col("product_name").contains("Gasoline") | col("product_name").contains("Blend"), "Gasoline")
                        .otherwise("Other Petroleum Product"))

        # 3. Area Dimension (With Surrogate Key and Categorization)
        area_supply = supply_df.select("area")
        area_movements = movements_df.select(col("country_code").alias("area"))
        
        dim_area_raw = area_supply.union(area_movements).distinct()
        
        dim_area = dim_area_raw \
            .withColumn("area_id", abs(crc32(col("area"))).cast("bigint")) \
            .withColumnRenamed("area", "area_name") \
            .withColumn("area_type",
                        when(col("area_name") == "U.S.", "Country")
                        .when(col("area_name").startswith("PADD"), "PADD District")
                        .otherwise("Other/Unknown"))

        # ==========================================
        # FACT TABLE GENERATION
        # ==========================================
        
        # Step 1: Prepare Weekly Prices
        diesel_prices = prices_df.filter(col("product_name").contains("No 2 Diesel")) \
            .withColumn("join_year", year(col("date"))) \
            .withColumn("join_week", weekofyear(col("date"))) \
            .withColumn("join_month", date_format(col("date"), "yyyy-MM")) \
            .select(
                col("date").alias("price_date"), 
                col("product_name").alias("price_product"), 
                "price_usd_per_gal", 
                "join_year", "join_week", "join_month"
            )

        # Step 2: Prepare Weekly Supply
        diesel_supply = supply_df.filter(
            (col("process_name") == "Ending Stocks") &
            (col("area") == "U.S.") &
            (col("product_name").contains("Distillate Fuel Oil"))
        ).withColumn("join_year", year(col("date"))) \
         .withColumn("join_week", weekofyear(col("date"))) \
         .groupBy("join_year", "join_week", "area") \
         .agg(_sum("volume_k_barrels").alias("total_stock_k_barrels"))

        # Step 3: Prepare Monthly Macro Context
        diesel_movements = movements_df.filter(
            (col("country_code") == "U.S.") &
            (col("product_name").contains("Distillate Fuel Oil")) &
            (col("description").contains("Net Imports"))
        ).withColumn("join_month", date_format(col("month_date"), "yyyy-MM")) \
         .groupBy("join_month", "country_code") \
         .agg(_sum("volume_k_barrels_per_day").alias("monthly_net_imports_k_bpd"))

        # Step 4: Consolidate Fact Table
        fact_intermediate = diesel_prices.join(
            diesel_supply, 
            (diesel_prices.join_year == diesel_supply.join_year) & 
            (diesel_prices.join_week == diesel_supply.join_week), 
            how="inner"
        )
        
        fact_with_macro = fact_intermediate.join(
            diesel_movements,
            fact_intermediate.join_month == diesel_movements.join_month,
            how="left"
        ).fillna({"monthly_net_imports_k_bpd": 0.0})

        # Step 5: Replace Natural Keys (Strings) with Surrogate Keys (IDs)
        fact_diesel_market = fact_with_macro \
            .join(dim_product, fact_with_macro.price_product == dim_product.product_name, "left") \
            .join(dim_area, fact_with_macro.area == dim_area.area_name, "left") \
            .select(
                col("price_date").alias("date"), 
                col("product_id"), 
                col("area_id"),
                round(col("price_usd_per_gal"), 3).alias("price_usd_per_gal"),
                col("total_stock_k_barrels"),
                round(col("monthly_net_imports_k_bpd"), 2).alias("monthly_net_imports_k_bpd")
            )

        # ==========================================
        # DATA PERSISTENCE
        # ==========================================
        base_gold_path = "/opt/airflow/datalake/gold"
        
        def safe_save_parquet(df, folder_name):
            target_path = f"{base_gold_path}/{folder_name}"
            if os.path.exists(target_path):
                shutil.rmtree(target_path)
            df.write.mode("overwrite").parquet(target_path)
            print(f"[INFO] Successfully saved {folder_name} to Gold Layer.")

        safe_save_parquet(dim_date, "dim_date")
        safe_save_parquet(dim_product, "dim_product")
        safe_save_parquet(dim_area, "dim_area")
        safe_save_parquet(fact_diesel_market, "fact_diesel_market")
        
        print("[INFO] Gold Star Schema pipeline completed successfully.")

    except Exception as e:
        print(f"[ERROR] Critical failure in Gold Star Schema Pipeline: {e}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    build_gold_star_schema()