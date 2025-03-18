import sys

from awsglue.utils import getResolvedOptions

from common import constants
from common.constants import (TABLE_DATAFRAMES,
                              VAR_S3_ENTITY_MASTER_TABLE_NAME,
                              VAR_S3_EXECUTION_PLAN_TABLE_NAME,
                              VAR_S3_RULE_MASTER_TABLE_NAME)
from common.custom_logger import get_logger
from common.spark_config import createSparkSession
from utilities.common_functions import (fetch_entity_columns,
                                        fetch_filtered_rules, fetch_rules,
                                        join_execution_plan_with_rules)
from utilities.dq__execution import execute_data_quality_checks
from utilities.table_loader import (fetch_tables, filter_config_by_entity,
                                    load_entity_data)
from utilities.validation import (execute_validations, generate_validation,
                                  load_metadata)

# take entity_id as input paramater and save it in constants
args = getResolvedOptions(sys.argv, ['entity_id'])
constants.VAR_ENTITY_ID = args['entity_id']
VAR_BATCH_ID = args['batch_id']
logger = get_logger()
spark = createSparkSession()


def dq_check(VAR_ENTITY_ID, VAR_BATCH_ID):
    # info
    logger.info(f"Starting DQ process for entity_id {VAR_ENTITY_ID}")
    # Loading config tables
    (
        entity_master_df,
        execution_plan_df,
        rule_master_df
    ) = fetch_tables(
        spark,
        VAR_S3_ENTITY_MASTER_TABLE_NAME,
        VAR_S3_EXECUTION_PLAN_TABLE_NAME,
        VAR_S3_RULE_MASTER_TABLE_NAME
    )
    # Filtering Config tables to load data according to VAR_ENTITY_ID
    entity_master_filtered_df = filter_config_by_entity(
        entity_master_df, VAR_ENTITY_ID)
    TABLE_DATAFRAMES['dq_entity_master'] = entity_master_filtered_df
    execution_plan_filtered_df = filter_config_by_entity(
        execution_plan_df, VAR_ENTITY_ID)
    TABLE_DATAFRAMES['dq_execution_plan'] = execution_plan_filtered_df
    # Filter rules from rule_master_df based on rule list
    # fetch from execution_plan_df
    rule_list = fetch_rules(execution_plan_filtered_df)
    rule_master_filtered_df = fetch_filtered_rules(rule_list, rule_master_df)
    TABLE_DATAFRAMES['dq_rule_master'] = rule_master_filtered_df

    # Performing validations on filtered df
    metadata = load_metadata()
    validations = generate_validation(
        TABLE_DATAFRAMES, metadata, VAR_ENTITY_ID
    )
    validation_status = execute_validations(validations)
    if not validation_status:
        logger.error("Validation process has been failed. "
                     "Cannot proceed with DQ check process. STATUS:'FAILED'")
        return False
    # Fetch entity path from entity_master_filtered_df and
    # fetch entity_data_df from file_path
    entity_columns_list = fetch_entity_columns(
        entity_master_filtered_df, VAR_ENTITY_ID
    )

    # Load entity_data_df
    entity_file_path = entity_columns_list[0]
    entity_metadata = entity_columns_list[3]
    entity_data_df = load_entity_data(
        spark, entity_file_path, entity_metadata, VAR_BATCH_ID
    )
    # Combine execution_plan_filtered_df with rule_master_filtered_df
    execution_plan_with_rule_df = join_execution_plan_with_rules(
        execution_plan_filtered_df, rule_master_filtered_df
    )
    # Execute dq on actual entity_data_df
    execute_data_quality_checks(
        spark, execution_plan_with_rule_df, entity_data_df,
        entity_columns_list,  VAR_BATCH_ID
    )
