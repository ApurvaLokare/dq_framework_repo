# Create s3 Table for dq_execution_result
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS s3tablesbucket. {namespace}. {execution_result_table_name} (
            execution_id INT NOT NULL,
            er_id INT NOT NULL,  
            ep_id INT NOT NULL,
            entity_id INT NOT NULL,
            entity_name STRING NOT NULL,
            batch_id STRING NOT NULL,        
            rule_id INT NOT NULL,
            rule_name STRING NOT NULL,
            column_name STRING,
            is_critical STRING NOT NULL,
            parameter_value STRING,
            total_records INT NOT NULL,
            failed_records_count INT NOT NULL,
            er_status STRING NOT NULL,
            error_records_path STRING,
            error_message STRING,
            execution_timestamp STRING NOT NULL,
            year STRING NOT NULL,
            month STRING NOT NULL,
            day STRING NOT NULL
            ) USING iceberg
            PARTITIONED BY (year, month, day, entity_id, batch_id)
    """)


