# Entity id default set to none
VAR_ENTITY_ID = None

# config table paths
VAR_S3_RULE_MASTER_TABLE_NAME = "s3tablesbucket.dq_testdb2.dq_rule_master"
VAR_S3_ENTITY_MASTER_TABLE_NAME = "s3tablesbucket.dq_testdb2.dq_entity_master"
VAR_S3_EXECUTION_PLAN_TABLE_NAME = (
    "s3tablesbucket.dq_testdb2.dq_execution_plan"
)
VAR_S3_EXECUTION_RESULT_TABLE_NAME = (
    "s3tablesbucket.dq_testdb2.dq_execution_result"
)

# s3table bucket ARN id
DQ_BUCKET = "arn:aws:s3tables:us-east-1:971996090633:bucket/dq-framework"


# result store paths
VAR_INVALID_RECORD_PATH = "s3://dq-results-store/Output/bad_records/"
VAR_VALID_RECORD_PATH = "s3://dq-results-store/Output/good_records/"

# Directory path containing JSON files
METADATA_PATH = "config_table_metadata"


# Required table metadata
REQUIRED_METADATA_FILES = {
    "dq_entity_master": "dq_entity_master.json",
    "dq_rule_master": "dq_rule_master.json",
    "dq_execution_plan": "dq_execution_plan.json",
}


# validation to be performed
DATAYPE_VALIDATION = "Column data type validation"
NULLABLE_VALIDATION = "Nullable constraint validation"
PRIMARY_KEY_UNIQUENESS_VALIDATION = "Primary key uniqueness validation"


# DataFrames dictionary to store table data
TABLE_DATAFRAMES = {
    "dq_entity_master": None,
    "dq_rule_master": None,
    "dq_execution_plan": None,
}

# Status fail or success
STATUS_FAIL = "FAILED"
STATUS_PASS = "SUCCESS"
STATUS_EXCEPTION = "EXCEPTION"
# Inbuilt rule Module name
RULE_MODULE = "rules.inbuilt_rules"
# column name constants
COL_RULE_ID = "rule_id"
COL_RULE_NAME = "rule_name"
COL_EP_ID = "ep_id"
COL_ENTITY_ID = "entity_id"
COL_INPUT_FILE_PATH = "input_file_path"
COL_OUTPUT_FILE_PATH = "output_file_path"
COL_ERROR_FILE_PATH = "error_file_path"
COL_ENTITY_METADATA = "entity_metadata"
COL_ENTITY_NAME = "entity_name"
COL_ISACTIVE = "is_active"
# Properties constants
PROP_COLUMNS = "columns"
PROP_FOREIGN_KEY = "foreign_key"
PROP_NAME = "name"
PROP_REFERENCES = "references"
PROP_TYPE = "type"
PROP_NULLABLE = "nullable"
PROP_PRIMARY_KEY = "primary_key"
PROP_TABLE = "table"
PROP_COLUMN = "column"
PROP_SIZE = "size"
# Entity Information
COLUMN_MAPPING = {
    "entity_name": ["entity_name", "name", "entity_title"],
    "entity_type": ["entity_type", "type"],
    "source_name": ["source_name", "source"],
    "source_type": ["source_type", "source_category"],
    "last_updated_date": ["last_updated_date", "updated_at"],
}
# Supported formats to read entity df
SUPPORTED_FILE_FORMATS = {"csv", "parquet", "json", "orc", "avro", "jdbc"}
FILE_SYSTEM = "s3"
PROP_FILE_FORMAT = "file_format"
PROP_FORMAT_DETAILS = "format_details"
