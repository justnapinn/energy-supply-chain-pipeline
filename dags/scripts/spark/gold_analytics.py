from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, year, month, quarter, weekofyear, dayofweek, lit

def build_gold_star_schema():
    """
    Orchestrates the transformation of silver layer data into a curated gold layer 
    using a Star Schema architecture. This process generates dimension and fact 
    tables optimized for analytical reporting and BI consumption.
    """
    spark = SparkSession.builder \
        .appName("Gold_Star_Schema_Transformation") \
        .master("local[*]") \
        .getOrCreate()

    try:
        print("--- Starting Full Star Schema Transformation ---")
        
        # 1. Load data from the Silver Layer (Parquet format)
        prices_df = spark.read.parquet("/opt/airflow/datalake/silver/petroleum_prices")
        supply_df = spark.read.parquet("/opt/airflow/datalake/silver/supply_estimates")

        # 2.1 Generate dim_date: Provides temporal attributes for time-series analysis
        # Extracts unique dates from both datasets to ensure continuous time coverage
        dates_prices = prices_df.select("date")
        dates_supply = supply_df.select("date")
        dim_date = dates_prices.union(dates_supply).distinct() \
            .withColumn("year", year(col("date"))) \
            .withColumn("month", month(col("date"))) \
            .withColumn("quarter", quarter(col("date"))) \
            .withColumn("week_of_year", weekofyear(col("date"))) \
            .withColumn("day_of_week", dayofweek(col("date")))

        # 2.2 Generate dim_product: Maintains unique product master data
        products_prices = prices_df.select("product_name").distinct()
        products_supply = supply_df.select("product_name").distinct()
        dim_product = products_prices.union(products_supply).distinct()

        # 2.3 Generate dim_area: Maintains geographic/regional master data
        # Note: Primary area attributes are sourced from the supply dataset
        dim_area = supply_df.select("area").distinct()

        # FACT TABLE GENERATION
    
        # Filter and prepare metrics for Diesel prices
        diesel_prices = prices_df.filter(col("product_name").contains("No 2 Diesel")) \
            .select(col("date").alias("price_date"), "price_usd_per_gal")

        # Filter and prepare metrics for U.S. Distillate Fuel Oil inventory (Ending Stocks)
        diesel_supply = supply_df.filter(
            (col("process_name") == "Ending Stocks") &
            (col("area") == "U.S.") &
            (col("product_name").contains("Distillate Fuel Oil"))
        ).select(col("date").alias("supply_date"), col("volume_k_barrels").alias("total_stock_k_barrels"))

        # Consolidate metrics into a centralized Fact Table via an Inner Join on date
        fact_diesel_market = diesel_prices.join(
            diesel_supply, 
            diesel_prices.price_date == diesel_supply.supply_date, 
            how="inner"
        ).select(
            col("price_date").alias("date"),
            round(col("price_usd_per_gal"), 3).alias("price_usd_per_gal"),
            col("total_stock_k_barrels")
        )

        base_gold_path = "/opt/airflow/datalake/gold"
        
        # Write Dimension Tables
        dim_date.write.mode("overwrite").parquet(f"{base_gold_path}/dim_date")
        dim_product.write.mode("overwrite").parquet(f"{base_gold_path}/dim_product")
        dim_area.write.mode("overwrite").parquet(f"{base_gold_path}/dim_area")
        
        # Write Fact Table
        fact_diesel_market.write.mode("overwrite").parquet(f"{base_gold_path}/fact_diesel_market")
        
        print(f"Successfully built Star Schema with Fact and Dimensions at {base_gold_path}")

    except Exception as e:
        print(f"Critical error in Gold Star Schema Pipeline: {e}")
        raise e
    finally:
        # Terminate Spark session to release cluster resources
        spark.stop()

if __name__ == "__main__":
    build_gold_star_schema()