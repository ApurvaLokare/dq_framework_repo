import hashlib
import json
import time

from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException

from common.constants import (COL_ENTITY_ID, COL_ENTITY_METADATA,
                              COL_ENTITY_NAME, COL_EP_ID, COL_ERROR_FILE_PATH,
                              COL_INPUT_FILE_PATH, COL_ISACTIVE,
                              COL_OUTPUT_FILE_PATH, COL_RULE_ID, COL_RULE_NAME,
                              PROP_FILE_FORMAT, PROP_FORMAT_DETAILS,
                              VAR_S3_ENTITY_MASTER_TABLE_NAME,
                              VAR_S3_EXECUTION_PLAN_TABLE_NAME,
                              VAR_S3_RULE_MASTER_TABLE_NAME)
from common.custom_logger import get_logger


# Extracts the active execution plan from the provided DataFrame
# by filtering rows where
# is_active is "Y". Converts the filtered data into a list of tuples
# for further processing.
def get_active_execution_plans(execution_plan_with_rule_df):
    try:
        logger = get_logger()
        # Select only necessary columns and convert to list of tuples
        plan_list = (
            execution_plan_with_rule_df
            .filter(col(COL_ISACTIVE) == "Y")
            .rdd.map(tuple)
            .collect()
        )
        logger.info(
            f"[DQ_GET_ACTIVE_PLANS] Active execution plans "
            f"fetched successfully from '{VAR_S3_EXECUTION_PLAN_TABLE_NAME}' "
            f"table."
        )
        return plan_list
    except Exception as e:
        logger.error(
            f"[DQ_GET_ACTIVE_PLANS] Exception occurred while"
            f"fetching active execution plans from table "
            f"'{VAR_S3_EXECUTION_PLAN_TABLE_NAME}'. "
            f"Function : fetch_execution_plan(). "
            f"Exception - {e}."
        )
        return None


# Merges the execution plan DataFrame with the rules DataFrame using
# 'rule_id' as the key.
# Drops duplicate or unnecessary columns and orders the result by 'ep_id'.
def join_execution_plan_with_rules(execution_plan_df, rules_df):
    try:
        logger = get_logger()
        execution_plan_with_rules_df = (
            execution_plan_df.join(
                rules_df.select(COL_RULE_ID, COL_RULE_NAME),
                execution_plan_df.rule_id == rules_df.rule_id,
                "inner"
            )
            .drop(execution_plan_df.last_update_date)
            .drop(rules_df.rule_id)
            .orderBy(COL_EP_ID)
        )
        logger.info(
            "[DQ_JOIN_PLANS_AND_RULES] Execution plans and rules joined "
            "successfully for data quality processing."
        )
        return execution_plan_with_rules_df
    except Exception as e:
        logger.error(
            f"[DQ_JOIN_PLANS_AND_RULES] Exception occured while"
            f"joining Execution plans and required rules. "
            f"Function : join_execution_plan_with_rules(). "
            f"Exception - {e}."
        )
        raise Exception(e)


# fetch path from entity master table path
def fetch_entity_columns(entity_filtered_master_df, entity_id):
    try:
        logger = get_logger()

        # Fetch entity details efficiently
        result = (
            entity_filtered_master_df
            .filter(col(COL_ENTITY_ID) == entity_id)
            .select(
                COL_INPUT_FILE_PATH,
                COL_OUTPUT_FILE_PATH,
                COL_ERROR_FILE_PATH,
                COL_ENTITY_METADATA,
                COL_ENTITY_NAME
            )
            .limit(1)
            .collect()
        )

        if not result:
            logger.error(
                f"[FETCH_ENTITY_PATH] Required columns not found "
                f"in table {VAR_S3_ENTITY_MASTER_TABLE_NAME} "
                f"for entity_id: {entity_id}"
            )
            return None  # Return None if entity_id is not found

        # Convert Row to dictionary for easy access
        entity_data = list(result[0])

        logger.info(
            f"[FETCH_ENTITY_PATH] Required columns successfully fetched "
            f"from table '{VAR_S3_ENTITY_MASTER_TABLE_NAME}' "
            f"for entity_id: {entity_id}. "
        )

        return entity_data

    except Exception as e:
        logger.error(
            f"[FETCH_ENTITY_PATH] Exception occurred while fetching "
            f"required columns from table '{VAR_S3_ENTITY_MASTER_TABLE_NAME}' "
            f"for entity_id: {entity_id}. "
            f"Function : fetch_entity_columns(). "
            f"Exception - {e}"
        )
        raise Exception(e)


# Fetch required rules from execution plan df
def fetch_rules(execution_plan_filtered_df):
    try:
        logger = get_logger()
        # Fetch distinct rule_ids from the dataframe and collect as a list
        # Select distinct rule_ids using DataFrame API
        rule_list_df = (
            execution_plan_filtered_df.select(col(COL_RULE_ID)).distinct()
        )
        # Collect rule IDs efficiently using toLocalIterator() (prevents OOM)
        rule_list = (
            [row[COL_RULE_ID] for row in rule_list_df.toLocalIterator()]
        )

        if not rule_list:
            logger.error(
                f"[FETCH_RULES] Rules does not exists "
                f"in table '{VAR_S3_EXECUTION_PLAN_TABLE_NAME}' "
            )
            return []
        # Log success
        logger.info(f"[FETCH_RULES] List of required rules successfully "
                    f"fetched from table '{VAR_S3_EXECUTION_PLAN_TABLE_NAME}' "
                    )
        return rule_list
    except Exception as e:
        # Log error if something goes wrong
        logger.error(f"[FETCH_RULES] Exception occurred while "
                     f"fetching required rules from "
                     f"table '{VAR_S3_EXECUTION_PLAN_TABLE_NAME}' ."
                     f"Function : fetch_rules()"
                     f"Exception - {e}")
        raise Exception(e)


# Fetch required rules filtered rules maste df
def fetch_filtered_rules(rule_list, rule_master_df):
    try:
        logger = get_logger()
        if not rule_list:
            logger.error(
                "[FETCH_FILTERED_RULES] Rule list is empty, "
                "returning empty DataFrame."
            )
            # Return an empty DataFrame with the same schema
            return rule_master_df.limit(0)

        # Filter the rule_master_df for the given list of rule_ids
        rule_master_filtered_df = rule_master_df.filter(
            rule_master_df[COL_RULE_ID].isin(rule_list)
        )
        # Log success
        logger.info(
            f"[FETCH_FILTERED_RULES] {len(rule_list)} Required rules "
            f"successfully from table '{VAR_S3_RULE_MASTER_TABLE_NAME}'"
        )
        return rule_master_filtered_df
    except Exception as e:
        # Log error if filtering fails
        logger.error(f"[FETCH_FILTERED_RULES] Exception while filtering "
                     f"required rules from table "
                     f"'{VAR_S3_RULE_MASTER_TABLE_NAME}'. "
                     f"Function : fetch_filtered_rules(). "
                     f"Exception - {e}")
        raise Exception(e)


# Extract file format details
def extract_file_format_details(entity_metadata_config):
    """
    Extracts the file format and format details from the given 
    entity metadata configuration.

    Args:
        entity_metadata_config (str or dict): JSON string or dictionary 
        containing metadata related to file format.

    Returns:
        tuple: (file_format, format_details)
               - file_format (str): The format of the file 
               (e.g., "parquet", "csv").
               - format_details (dict): Additional format-specific options 
               if available, else None.
    """
    try:
        logger = get_logger()  # Initialize the logger for logging messages.

        # Attempt to parse the entity metadata configuration 
        # if it is in JSON string format.
        try:
            entity_metadata_config = json.loads(entity_metadata_config)
        except json.JSONDecodeError as e:
            # Log an error if the JSON parsing fails.
            logger.error(f"[EXTRACT FORMAT DETAILS] Invalid JSON format "
                         f"provided in table "
                         f"'{VAR_S3_ENTITY_MASTER_TABLE_NAME}' in "
                         f"column '{COL_ENTITY_METADATA}'. "
                         f"Error - {e}")
            return None, None

        # Retrieve the file format from the metadata configuration.
        file_format = entity_metadata_config.get(PROP_FILE_FORMAT)
        if not file_format:
            # Log an error if the file format key is missing.
            logger.error(f"[EXTRACT FORMAT DETAILS] Missing "
                         f"'{PROP_FILE_FORMAT}' in column "
                         f"'{COL_ENTITY_METADATA}' in table "
                         f"'{VAR_S3_ENTITY_MASTER_TABLE_NAME}'")
            return None, None

        # Retrieve additional format details if available.
        format_details = entity_metadata_config.get(PROP_FORMAT_DETAILS, {})
        if not isinstance(format_details, dict):
            # Log an error if format details are not in the 
            # expected dictionary format.
            logger.error(f"[EXTRACT FORMAT DETAILS] Incorrect format details "
                         f"provided in table "
                         f"'{VAR_S3_ENTITY_MASTER_TABLE_NAME}' in "
                         f"column '{COL_ENTITY_METADATA}'")
            return file_format, None

        return file_format, format_details

    except Exception as e:
        # Log any unexpected exception that occurs during execution.
        logger.error(f"[EXTRACT FORMAT DETAILS] Exception occurred while "
                     f"extracting format details. "
                     f"Function: extract_file_format_details(). "
                     f"Exception - {e}")
        return None, None  # Return None values in case of failure.


# Function to generate the unique er_id using timestamp and hash
def generate_er_id(var_entity_id, var_plan_id):
    """
    Generate a unique var_er_id based on entity_id, plan_id,
    and current timestamp.
    :param var_entity_id: Entity ID
    :param var_plan_id: Plan ID
    :return: A 6-digit unique identifier
    """
    hash_value = hashlib.sha256(
        f"{var_entity_id}-{var_plan_id}-{int(time.time() * 1000)}".encode()
    ).hexdigest()
    return int(hash_value, 16) % 900000 + 100000


# Function to genrate the unique execution id using timestamp, entity_id
# and hash
def generate_exec_id(var_entity_id):
    """
    Generate a unique var_exec_id based on entity_id
    and current timestamp.
    :param var_entity_id: Entity ID
    :return: A 6-digit unique identifier
    """
    hash_value = hashlib.sha256(
        f"{var_entity_id}-{int(time.time() * 1000)}".encode()
    ).hexdigest()
    return int(hash_value, 16) % 900000 + 100000


# Function to fetch the schema from given table
def fetch_table_schema(spark, table_name):
    try:
        # Initialize logger for capturing logs
        logger = get_logger()
        if not spark.catalog.tableExists(table_name):
            raise Exception(f"Configuration table '{table_name}' does  "
                            f"not exist! Cannot proceed with DQ process.")
        logger.info(f"[FETCH_TABLE_SCHEMA] Configuration table "
                    f"'{table_name}' loaded successfully")
        df = spark.read.table(table_name)
        return df.schema
    except Exception as e:
        raise Exception(e)
