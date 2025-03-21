import fsspec
from pyspark.sql.functions import col

from common.constants import (COL_ENTITY_ID, COLUMN_MAPPING, FILE_SYSTEM,
                              PROP_SIZE, PROP_TYPE, SUPPORTED_FILE_FORMATS)
from common.custom_logger import get_logger
from utilities.common_functions import extract_format_details

# from utilities.common_functions import detect_file_format


# Load configuration table data
def filter_config_by_entity(df, entity_id):
    """
    Filters the given DataFrame for a specific entity_id and selects relevant
    columns.

    Args:
        df (DataFrame): Input Spark DataFrame containing entity data.
        entity_id (str or int): The entity ID to filter by.

    Returns:
        DataFrame or None: Filtered DataFrame or None if no records are found.
    """
    try:
        logger = get_logger()
        # Filter the DataFrame for the specific entity_id
        filtered_df = df.filter(col(COL_ENTITY_ID) == entity_id)

        # Check if DataFrame is empty
        # if filtered_df.rdd.isEmpty():
        if filtered_df.isEmpty():
            logger.warning(
                f"[CONFIG LOADER] No records found for entity_id: {entity_id}"
            )
            return None

        # Get actual available column names in df
        df_columns = set(df.columns)
        actual_columns = {
            key: next((col for col in values if col in df_columns), None)
            for key, values in COLUMN_MAPPING.items()
        }

        # Remove None values (columns that don't exist in df)
        actual_columns = {k: v for k, v in actual_columns.items() if v}

        # Select available columns
        """
        selected_df = filtered_df.select(
            [col(col_name) for col_name in actual_columns.values()]
        ).distinct()
        """
        selected_df = (
            filtered_df.selectExpr(*actual_columns.values())
        )
        # Collect and convert to dictionary
        """
        entity_info = (
            selected_df.collect()[0].asDict()
            if selected_df.count() > 0
            else {}
        )"""
        collected_data = selected_df.collect()
        entity_info = collected_data[0].asDict() if collected_data else {}

        # Format output for logging
        entity_details_str = ", ".join(
            [
                f"{key}: {entity_info.get(v, 'Unknown')}"
                for key, v in actual_columns.items()
            ]
        )

        logger.info(f"Entity {entity_id} details: {entity_details_str}")

        return filtered_df

    except Exception as e:
        logger.error(
            f"[CONFIG LOADER] Error filtering DataFrame for "
            f"entity_id: {entity_id} - {e}"
        )
        return None


# load actual entity data
def fetch_tables(
    spark,
    entity_master_table_name,
    execution_plan_table_name,
    rule_master_table_name
):
    """
    Fetches data from the specified Iceberg tables.

    Args:
        spark (SparkSession): The Spark session.
        entity_master_table_name (str): Name of the entity master table.
        execution_plan_table_name (str): Name of the execution plan table.
        execution_result_table_name (str): Name of the execution result table.
        rule_master_table_name (str): Name of the rule master table.

    Returns:
        tuple: DataFrames corresponding to the input table names.
               Returns (None, None, None, None) in case of an error.
    """
    try:
        logger = get_logger()
        logger.info("[FETCH_TABLE] Starting entity data loading process...")

        entity_master_df = spark.read.table(entity_master_table_name)
        execution_plan_df = spark.read.table(execution_plan_table_name)
        rule_master_df = spark.read.table(rule_master_table_name)

        logger.info(
            f"[FETCH_TABLE] Successfully loaded  "
            f"table: {entity_master_table_name}"
        )
        logger.info(
            f"[FETCH_TABLE] Successfully loaded  "
            f"table: {execution_plan_table_name}"
        )
        logger.info(
            f"[FETCH_TABLE] Successfully loaded  "
            f"table:{rule_master_table_name}"
        )

        return (
            entity_master_df,
            execution_plan_df,
            rule_master_df
        )
    except Exception as e:
        logger.error(f" Error loading entity data: {e}")
        return None, None, None


def load_entity_data(spark, base_path, entity_metadata_config, batch_id):
    try:
        logger = get_logger()
        full_path = f"{base_path}/{batch_id}/"

        file_format, format_options = extract_format_details(
            entity_metadata_config
        )

        fs = fsspec.filesystem(FILE_SYSTEM)

        files = files = [
            file for file in fs.ls(full_path, detail=True)
            if file[PROP_TYPE] == "file" and file[PROP_SIZE] > 0
        ]

        if not files:
            logger.error(f"[DATA LOADER] No files found at : "
                         f"path {full_path}")
            return None

        if not file_format or file_format not in SUPPORTED_FILE_FORMATS:
            logger.error(
                f"[DATA LOADER] Unknown file format or no file format "
                f"provided. Please provide supported format. "
                f"Supported formats: {SUPPORTED_FILE_FORMATS}"
            )
            return None

        if format_options:
            df = (
                spark.read.format(file_format).options(**format_options)
                .load(full_path)
            )
        else:
            df = spark.read.format(file_format).load(full_path)

        logger.info(f"[DATA LOADER] Successfully loaded dataframe "
                    f"as {file_format.upper()} from path {full_path}.")
        return df
    except Exception as e:
        logger.error(f"[DATA LOADER] Exception occurred in "
                     f"load_entity_data():{e}.")
        return None
