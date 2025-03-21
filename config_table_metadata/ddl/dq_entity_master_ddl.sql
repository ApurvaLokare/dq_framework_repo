CREATE TABLE database.dq_entity_master (
    entity_id STRING NOT NULL,
    entity_name STRING NOT NULL,
    entity_type STRING NOT NULL,
    source_name STRING NOT NULL,
    source_type STRING NOT NULL,
    number_of_columns STRING,
    file_path STRING NOT NULL,
    delimiter STRING NOT NULL,
    last_updated_date STRING NOT NULL 

)
LOCATION 's3://bucket/dq_entity_master/'
TBLPROPERTIES ('table_type' = 'ICEBERG');
