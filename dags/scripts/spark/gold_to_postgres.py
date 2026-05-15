import os
import psycopg2
from pyspark.sql import SparkSession

def get_db_credentials():
    """
    Retrieves database credentials securely via Environment Variables.
    These variables are injected by the Airflow BashOperator using Jinja templating.
    This architecture prevents Airflow dependency injection issues in standalone Spark scripts.
    """
    return {
        "host": os.environ.get("DB_HOST", "postgres"),
        "port": int(os.environ.get("DB_PORT", 5432)),
        "database": os.environ.get("DB_NAME", "airflow"),
        "user": os.environ.get("DB_USER", "airflow"),
        "password": os.environ.get("DB_PASS", "airflow")
    }

def execute_upsert_merge():
    """
    Executes Phase 2 of the database load process utilizing UPSERT operations.
    Idempotency is guaranteed via DISTINCT ON and ON CONFLICT.
    Queries are updated to handle BIGINT surrogate keys and descriptive attributes.
    """
    print("[INFO] Phase 2: Executing UPSERT Merge via PostgreSQL")
    db_config = get_db_credentials()
    
    try:
        conn = psycopg2.connect(
            host=db_config["host"], 
            port=db_config["port"], 
            database=db_config["database"], 
            user=db_config["user"], 
            password=db_config["password"]
        )
        cur = conn.cursor()
        
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
                INSERT INTO dim_product (product_id, product_name, product_category)
                SELECT DISTINCT ON (product_id) product_id, product_name, product_category 
                FROM dim_product_staging
                ORDER BY product_id
                ON CONFLICT (product_id) DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    product_category = EXCLUDED.product_category;
            """,
            "dim_area": """
                INSERT INTO dim_area (area_id, area_name, area_type)
                SELECT DISTINCT ON (area_id) area_id, area_name, area_type 
                FROM dim_area_staging
                ORDER BY area_id
                ON CONFLICT (area_id) DO UPDATE SET
                    area_name = EXCLUDED.area_name,
                    area_type = EXCLUDED.area_type;
            """,
            "fact_diesel_market": """
                INSERT INTO fact_diesel_market (date, product_id, area_id, price_usd_per_gal, total_stock_k_barrels, monthly_net_imports_k_bpd)
                SELECT DISTINCT ON (date, product_id, area_id) 
                    date, product_id, area_id, price_usd_per_gal, total_stock_k_barrels, monthly_net_imports_k_bpd 
                FROM fact_diesel_market_staging
                ORDER BY date, product_id, area_id
                ON CONFLICT (date, product_id, area_id) DO UPDATE SET
                    price_usd_per_gal = EXCLUDED.price_usd_per_gal,
                    total_stock_k_barrels = EXCLUDED.total_stock_k_barrels,
                    monthly_net_imports_k_bpd = EXCLUDED.monthly_net_imports_k_bpd;
            """
        }
        
        for table, query in upsert_queries.items():
            print(f"[INFO] Merging data for target table: {table}")
            cur.execute(query)
            
            # Drop the temporary staging table to free up database resources
            cur.execute(f"DROP TABLE IF EXISTS {table}_staging;")
            
        conn.commit()
        cur.close()
        conn.close()
        print("[INFO] Phase 2 completed successfully. All staging tables dropped.")
        
    except Exception as e:
        print(f"[ERROR] Database operation failed during UPSERT merge: {e}")
        raise e

def load_gold_to_postgres():
    """
    Phase 1: Loads curated Gold Layer Parquet files into PostgreSQL Staging tables.
    Utilizes PySpark JDBC writer for distributed database loading.
    """
    db_config = get_db_credentials()
    jdbc_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['database']}"
    
    db_properties = {
        "user": db_config["user"],
        "password": db_config["password"],
        "driver": "org.postgresql.Driver"
    }

    spark = SparkSession.builder \
        .appName("Gold_to_Postgres_Loader") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2") \
        .config("spark.driver.extraClassPath", "org.postgresql:postgresql:42.7.2") \
        .getOrCreate()

    try:
        print("[INFO] Phase 1: Loading Gold Layer to Staging Tables via JDBC")
        
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
            
            # Write data to a temporary staging table, overwriting any previous failed loads
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

    # Trigger Phase 2 execution once all staging tables are successfully loaded
    execute_upsert_merge()

if __name__ == "__main__":
    load_gold_to_postgres()