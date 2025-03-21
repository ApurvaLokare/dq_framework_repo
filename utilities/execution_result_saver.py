from common.constants import STATUS_PASS, VAR_S3_EXECUTION_RESULT_TABLE_NAME
from common.custom_logger import get_logger


# Saves execution results of data quality checks to an Iceberg table in S3,
# partitioned by date and entity_id.
# Logs success or failure messages based on the save operation.
def save_execution_result(result_df, entity_id):
    try:
        logger = get_logger()
        # Write the result DataFrame to the Iceberg table in append mode with
        result_df.write\
            .format("iceberg")\
            .mode("append")\
            .saveAsTable(VAR_S3_EXECUTION_RESULT_TABLE_NAME)
        # Log success message after successful save operation
        logger.info(
            f"[DQ_RESULT_SAVE] Result data saved for Entity id:{entity_id}. "
            f"STATUS:'{STATUS_PASS}'"
        )
    except Exception as e:
        # Log error if an exception occurs while saving the results
        logger.error(
            f"[DQ_RESULT_SAVE] Exception occured during saving the result "
            f"records for entity_id {entity_id}: {e}"
        )


# Saves records that fail data quality checks to a Parquet file in S3,
# partitioned by date and entity_id.
# Logs success or failure messages based on the save operation.
def save_invalid_records(
        invalid_records_df, entity_id, error_record_path, er_id, batch_id
        ):
    try:
        logger = get_logger()

        target_path = f"{error_record_path}/{batch_id}/{er_id}"
        # Write the DataFrame to the storage location
        invalid_records_df.write\
            .mode("append")\
            .format("parquet")\
            .option("compression", "snappy")\
            .save(target_path)
        # Log success message after saving bad records
        logger.info(
            f"[DQ_INVALID_RECORDS_SAVE] Invalid records saved for "
            f"entity_id {entity_id}. Error record path:{target_path} "
            f"STATUS:'{STATUS_PASS}'"
        )
    except Exception as e:
        # Log error if an exception occurs while saving bad records
        logger.error(
            f"[DQ_INVALID_RECORDS_SAVE] Exception occured in "
            f"save_bad_records() for entity_id {entity_id}: {e}"
        )


# Saves records that pass data quality checks to a Parquet file in S3,
# partitioned by date and entity_id.
# Logs success or failure messages based on the save operation.
def save_valid_records(valid_records_df, entity_id, output_path, batch_id):
    try:
        logger = get_logger()
        target_path = f"{output_path}/{batch_id}"
        # Write the DataFrame to the storage location in append mode
        valid_records_df.write\
            .mode("overwrite")\
            .format("parquet")\
            .option("compression", "snappy")\
            .save(target_path)
        # Log success message after saving good records
        logger.info(
            f"[DQ_VALID_RECORDS_SAVE] Valid records saved for "
            f"entity_id {entity_id}. Output path: {target_path}. "
            f"STATUS:'{STATUS_PASS}'"
        )
    except Exception as e:
        # Log error if an exception occurs while saving good records
        logger.error(
            f"[DQ_VALID_RECORDS_SAVE] Exception occured in "
            f"save_good_records() for entity_id {entity_id}: {e}"
        )
