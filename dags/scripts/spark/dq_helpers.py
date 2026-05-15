from pyspark.sql.functions import lit, current_timestamp

def apply_quarantine(df, valid_condition, error_reason_text, quarantine_path):
    """
    Separates valid and invalid records based on a condition.
    Invalid records are tagged with an error reason and timestamp, 
    then appended to the Quarantine Zone (Dead Letter Queue).
    
    Args:
        df (DataFrame): The input PySpark DataFrame.
        valid_condition (Column): The PySpark condition defining valid rows.
        error_reason_text (str): Description of why the row failed DQ check.
        quarantine_path (str): The storage path for quarantined data.
        
    Returns:
        DataFrame: A DataFrame containing only the valid records.
    """
    # 1. Filter valid and invalid data streams
    valid_df = df.filter(valid_condition)
    invalid_df = df.filter(~valid_condition) # The '~' negates the valid condition
    
    # 2. Check if there are any invalid records
    invalid_count = invalid_df.count()
    if invalid_count > 0:
        print(f"[WARN] Detected {invalid_count} anomalous records. Routing to Quarantine Zone.")
        
        # 3. Add metadata to quarantined records
        quarantine_df = invalid_df.withColumn("error_reason", lit(error_reason_text)) \
                                  .withColumn("quarantined_at", current_timestamp())
        
        # 4. Save to Dead Letter Queue (Append mode to keep history)
        quarantine_df.write.mode("append").parquet(quarantine_path)
        print(f"[INFO] Successfully archived invalid records to {quarantine_path}")
    else:
        print("[INFO] DQ Check Passed: No invalid records detected.")
        
    return valid_df