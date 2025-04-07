# Create s3 Table for dq_rule_master
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS s3tablesbucket. {namespace}. { rule_master_table_name } (
            rule_id BIGINT NOT NULL,  
            rule_name  STRING NOT NULL,
            rule_type STRING NOT NULL,
            rule_description STRING NOT NULL,
            last_updated_date DATE NOT NULL
        ) USING iceberg
    """)