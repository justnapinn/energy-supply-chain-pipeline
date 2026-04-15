import os
import psycopg2
from pyspark.sql import SparkSession

def execute_upsert_merge():
    """
    Executes the Phase 2 of the database load process.
    Connects to PostgreSQL to merge data from Staging tables into Target tables.
    Utilizes the UPSERT (INSERT ... ON CONFLICT) pattern and DISTINCT ON
    to guarantee idempotency and prevent Cardinality Violations.
    """
    print("[INFO] Phase 2: Executing UPSERT Merge via PostgreSQL")
    try:
        conn = psycopg2.connect(
            host="postgres", 
            port=5432, 
            database="airflow", 
            user="airflow", 
            password="airflow"
        )
        cur = conn.cursor()
        
        # Define UPSERT logic mapped to table definitions and constraints
        upsert_queries = {
            "dim_date": """
                INSERT INTO dim_date (date, year, month, quarter, week_of_year, day_of_week)
                SELECT DISTINCT ON (date) date, year, month, quarter, week_of_year, day_of_week 
                FROM dim_date_staging
                ORDER BY date
                ON CONFLICT (date) DO UPDATE SET
                    year = EXCLUDED.year, month = EXCLUDED.month, 
                    quarter = EXCLUDED.quarter, week_of_year = EXCLUDED.week_of_year, 
                    day_of_week = EXCLUDED.day_of_week;
            """,
            "dim_product": """
                INSERT INTO dim_product (product_name)
                SELECT DISTINCT ON (product_name) product_name 
                FROM dim_product_staging
                ORDER BY product_name
                ON CONFLICT (product_name) DO NOTHING;
            """,
            "dim_area": """
                INSERT INTO dim_area (area)
                SELECT DISTINCT ON (area) area 
                FROM dim_area_staging
                ORDER BY area
                ON CONFLICT (area) DO NOTHING;
            """,
            "fact_diesel_market": """
                INSERT INTO fact_diesel_market (date, product_name, area, price_usd_per_gal, total_stock_k_barrels)
                SELECT DISTINCT ON (date, product_name, area) 
                    date, product_name, area, price_usd_per_gal, total_stock_k_barrels 
                FROM fact_diesel_market_staging
                ORDER BY date, product_name, area
                ON CONFLICT (date, product_name, area) DO UPDATE SET
                    price_usd_per_gal = EXCLUDED.price_usd_per_gal,
                    total_stock_k_barrels = EXCLUDED.total_stock_k_barrels;
            """
        }
        
        # Execute merge operations and drop temporary staging tables
        for table, query in upsert_queries.items():
            print(f"[INFO] Merging data for target table: {table}")
            cur.execute(query)
            cur.execute(f"DROP TABLE IF EXISTS {table}_staging;")
            
        conn.commit()
        cur.close()
        conn.close()
        print("[INFO] Phase 2 completed. All staging tables dropped.")
        
    except Exception as e:
        print(f"[ERROR] Database operation failed during UPSERT merge: {e}")
        raise e

def load_gold_to_postgres():
    """
    Phase 1: Loads curated Gold Layer Parquet files into PostgreSQL Staging tables.
    """
    jdbc_url = "jdbc:postgresql://postgres:5432/airflow"
    db_properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver"
    }

    spark = SparkSession.builder \
        .appName("Gold_to_Postgres_Loader") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2") \
        .config("spark.driver.extraClassPath", "org.postgresql:postgresql:42.7.2") \
        .getOrCreate()

    try:
        print("[INFO] Phase 1: Loading Gold Layer to Staging Tables")
        
        tables = ["dim_date", "dim_product", "dim_area", "fact_diesel_market"]
        base_path = "/opt/airflow/datalake/gold"

        for table in tables:
            source_path = f"{base_path}/{table}"
            
            if not os.path.exists(source_path):
                print(f"[WARN] Source path not found for table {table}. Skipping operation.")
                continue
            
            print(f"[INFO] Staging data for {table}...")
            df = spark.read.parquet(source_path)
            
            staging_table = f"{table}_staging"
            df.write.jdbc(
                url=jdbc_url, 
                table=staging_table, 
                mode="overwrite", 
                properties=db_properties
            )
            print(f"[INFO] Successfully staged: '{staging_table}'")

    except Exception as e:
        print(f"[ERROR] Spark process failed during Staging load: {str(e)}")
        raise e
    finally:
        spark.stop()

    # Trigger Phase 2 execution
    execute_upsert_merge()

if __name__ == "__main__":
    load_gold_to_postgres()