# Create s3 Table for dq_execution_plan
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS s3tablesbucket. {namespace}. {execution_plan_table_name} (
            ep_id  BIGINT NOT NULL,
            rule_id BIGINT NOT NULL,
            entity_id BIGINT NOT NULL,
            column_name STRING,
            parameter_value STRING,
            is_critical STRING NOT NULL,
            is_active STRING NOT NULL,
            last_updated_date  DATE NOT NULL
        ) USING iceberg
    """)