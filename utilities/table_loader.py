import fsspec
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException

from common.constants import (COL_ENTITY_ID, COLUMN_MAPPING, FILE_SYSTEM,
                              PROP_SIZE, PROP_TYPE, SUPPORTED_FILE_FORMATS)
from common.custom_logger import get_logger
from utilities.common_functions import extract_file_format_details

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
        selected_df = (
            filtered_df.selectExpr(*actual_columns.values())
        )
        # Collect and convert to dictionary
        collected_data = selected_df.collect()
        entity_info = collected_data[0].asDict() if collected_data else {}

        # Format output for logging
        entity_details_str = ", ".join(
            [
                f"{key}: {entity_info.get(v, 'Unknown')}"
                for key, v in actual_columns.items()
            ]
        )

        logger.info(
            f"[CONFIG LOADER] Entity {entity_id} details: {entity_details_str}"
        )

        return filtered_df

    except Exception as e:
        logger.error(
            f"[CONFIG LOADER] Exception occurred while "
            f"filtering DataFrame for entity_id: {entity_id}. "
            f"Function : filter_config_by_entity(). "
            f"Exception - {e}"
        )
        raise Exception(e)


# Fetch config tables
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
        rule_master_table_name (str): Name of the rule master table.

    Returns:
        tuple: DataFrames corresponding to the input table names.
               Raise the exceptio in case of an error.
    """
    try:
        logger = get_logger()
        logger.info(
            "[FETCH_TABLE] Beginning the configuration "
            "tables loading process..."
        )
        # Try reading each table one by one to isolate failures
        for table in [
            entity_master_table_name,
            execution_plan_table_name,
            rule_master_table_name
        ]:
            if not spark.catalog.tableExists(table):
                raise Exception(f"Configuration table '{table}' does  "
                                "not exist!")

        entity_master_df = spark.read.table(entity_master_table_name)
        logger.info(f"[FETCH_TABLE] Configuration table "
                    f"'{entity_master_table_name}' loaded successfully.")

        execution_plan_df = spark.read.table(execution_plan_table_name)
        logger.info(f"[FETCH_TABLE] Configuration table "
                    f"'{execution_plan_table_name}' loaded successfully.")

        rule_master_df = spark.read.table(rule_master_table_name)
        logger.info(f"[FETCH_TABLE] Configuration table "
                    f"'{rule_master_table_name}' loaded successfully.")

        return (
            entity_master_df,
            execution_plan_df,
            rule_master_df
        )
    except Exception as e:
        logger.error(f"[FETCH_TABLE] Exception occurred while "
                     f"fetching configuration tables. "
                     f"Function : fetch_tables(). "
                     f"Exception - {e}")
        raise Exception(e)


# Load actual entity data
def load_entity_data(spark, base_path, entity_metadata_config, batch_id):
    """
    Loads entity data from the specified base path
    using metadata configurations.

    Args:
        spark: SparkSession object used to read data.
        base_path (str): Base directory where data files are stored.
        entity_metadata_config (dict): Metadata configuration containing
        file format and options.
        batch_id (str): Unique identifier for the batch, used to construct
        the file path.

    Returns:
        DataFrame: Spark DataFrame containing loaded data if successful,
        otherwise None.
    """
    try:
        logger = get_logger()  # Initialize logger for logging messages.

        # Construct the full file path for the given batch ID.
        full_path = f"{base_path}/{batch_id}/"

        # Extract file format and additional options from metadata config.
        file_format, format_options = extract_file_format_details(
            entity_metadata_config)

        # Initialize the file system using fsspec.
        fs = fsspec.filesystem(FILE_SYSTEM)

        # List all files in the specified directory and filter non-empty files.
        files = [
            file for file in fs.ls(full_path, detail=True)
            if file[PROP_TYPE] == "file" and file[PROP_SIZE] > 0
        ]

        # Log an error and return None if no valid files are found.
        if not files:
            logger.error(f"[DATA LOADER] No files detected at "
                         f"provided path: {full_path}")
            return None

        # Validate the extracted file format against supported formats.
        if not file_format or file_format not in SUPPORTED_FILE_FORMATS:
            logger.error(
                f"[DATA LOADER] Unknown file format or no file format "
                f"provided. Please provide a supported format. "
                f"Supported formats: {SUPPORTED_FILE_FORMATS}"
            )
            return None

        # Read data into a DataFrame using the extracted file format
        # and options.
        if format_options:
            df = (
                spark.read.format(file_format).options(**format_options)
                .load(full_path)
            )
        else:
            df = spark.read.format(file_format).load(full_path)

        # Log success message indicating data was loaded successfully.
        logger.info(f"[DATA LOADER] Data has been successfully loaded "
                    f"into a DataFrame from files at path {full_path} "
                    f"with given format {file_format}.")
        return df

    except Exception as e:
        # Log an error message in case of an exception and return None.
        logger.error(f"[DATA LOADER] Exception occurred while loading "
                     f"source data from path {full_path}. "
                     f"Function: load_entity_data(). Exception - {e}")
        raise Exception(e)
