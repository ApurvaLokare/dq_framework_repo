# Create s3 Table for dq_entity_master
    spark.sql (f"""
        CREATE TABLE IF NOT EXISTS s3tablesbucket. {namespace}. { entity_master_table_name } (
            entity_id BIGINT NOT NULL,  
            entity_name  STRING NOT NULL,
            entity_type STRING NOT NULL,
            source_name STRING NOT NULL,
            source_type STRING NOT NULL,
            input_file_path  STRING NOT NULL,
            output_file_path  STRING NOT NULL,
            error_file_path STRING NOT NULL,
            entity_metadata  STRING NOT NULL,
            last_updated_date DATE NOT NULL
        ) USING iceberg
    """)





