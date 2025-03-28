import json

from pyspark.sql.functions import col, length, to_date, trim

from common.constants import STATUS_FAIL, STATUS_PASS
from common.custom_logger import get_logger


# Checks for null values in the specified column of the DataFrame.
# Returns a DataFrame of null records, a success flag, and an error
# message if nulls are found.
def null_check(df, column_name):
    try:
        logger = get_logger()
        # Filter rows where the specified column has null values and
        # count the number of null records
        null_record_df = df.filter(col(column_name).isNull())
        null_count = null_record_df.count()

        if null_count > 0:
            # If null values are found, log an error message and return details
            error_message = (
                f"Rule 'Null Check' failed. "
                f"Column '{column_name}' contains {null_count} null values. "
            )
            logger.error(f"[DQ_RULE_EXECUTED] {error_message}. "
                         f"STATUS:'{STATUS_FAIL}'.")
            # return df with records failed for check with flag
            # and error message
            return (null_record_df, False, error_message, null_count)
        else:
            # If no null values are found, log success and return
            # a success flag
            logger.info(
                f"[DQ_RULE_EXECUTED] Rule 'Null Check' passed. "
                f"Column'{column_name}' contains no null values."
                f"STATUS:'{STATUS_PASS}' "
            )
            return (None, True, None, None)

    except Exception as e:
        # Handle any unexpected exceptions and return error message
        error_message = f"Exception occurred during null_check(): {e}"
        return ("EXCEPTION", False, error_message, None)


# Checks for empty string values in the specified column of the DataFrame.
# Returns a DataFrame of empty string records, a success flag, and an error
# message if empty strings are found.
def empty_string_check(df, column_name):
    try:
        logger = get_logger()
        # Count the number of records where the specified column contains
        # empty strings after trimming spaces
        empty_record_df = df.filter(trim(col(column_name)) == "")
        empty_count = empty_record_df.count()
        if empty_count > 0:
            # Log the error and return details
            error_message = (
                f"Rule 'Empty String Check' failed. "
                f"Column '{column_name}' contains {empty_count} "
                f"empty string values."
            )
            logger.error(f"[DQ_RULE_EXECUTED] {error_message}. "
                         f"STATUS:'{STATUS_FAIL}'.")
            # return df with records failed for check with flag and
            # error message
            return (empty_record_df, False, error_message, empty_count)
        else:
            # If no empty strings are found, log success and return a
            # success flag
            logger.info(
                f"[DQ_RULE_EXECUTED] Rule 'Empty String Check' passed. "
                f"Column '{column_name}' contains no empty string values. "
                f"STATUS:'{STATUS_PASS}'"
            )
            return (None, True, None, None)
    except Exception as e:
        # Handle any unexpected exceptions and return error message
        error_message = f"Exception occurred during empty_string_check(): {e}"
        return ("EXCEPTION", False, error_message, None)


# Checks the uniqueness of a primary key column in the DataFrame.
# Identifies and returns duplicate and null primary key values if found,
# along with a success flag and an error message.
def primary_key_uniqueness_check(df, primary_key_column):
    try:
        logger = get_logger()
        # Identify duplicate primary key values (excluding NULLs)
        duplicate_keys_df = (
            # Exclude NULLs for now
            df.filter(df[primary_key_column].isNotNull())
            .groupBy(primary_key_column)  # Group by primary key column
            .count()
            .filter(col("count") > 1)  # Filter values appears > 1
            .select(primary_key_column)  # Select only the primary key column
        )
        # Fetch duplicate records by joining with the DataFrame
        duplicate_record_df = df.join(
            duplicate_keys_df, on=primary_key_column, how="inner"
        ).dropDuplicates()
        duplicate_count = duplicate_record_df.count()
        # Identify NULL values in the primary key column
        null_record_df = (
            df.filter(df[primary_key_column].isNull()).dropDuplicates()
        )
        null_count = null_record_df.count()
        # Handle scenarios based on duplicates and NULL counts
        if duplicate_count > 0 and null_count > 0:
            error_message = (
                f"Rule 'Primary Key Uniqueness check' failed. "
                f"Primary key column '{primary_key_column}' "
                f"contains {duplicate_count} duplicate and "
                f"{null_count} null values."
            )
            logger.error(f"[DQ_RULE_EXECUTED] {error_message}. "
                         f"STATUS:'{STATUS_FAIL}'.")
            # return df with records failed for check with flag and
            # error message
            return (
                duplicate_record_df.union(null_record_df),
                False,
                error_message,
                (duplicate_count+null_count)
                )
        elif duplicate_count > 0:
            error_message = (
                f"Rule 'Primary Key Uniqueness check' failed. "
                f"Primary key column '{primary_key_column}' "
                f"contains {duplicate_count} duplicate values."
            )
            logger.error(f"[DQ_RULE_EXECUTED] {error_message}. "
                         f"STATUS:'{STATUS_FAIL}'.")
            # return df with records failed for check with flag and
            # error message
            return (duplicate_record_df, False, error_message, duplicate_count)
        elif null_count > 0:
            error_message = (
                f"Rule 'Primary Key Uniqueness check' failed. "
                f"Primary key column '{primary_key_column}' "
                f"contains {null_count} null values."
            )
            logger.error(f"[DQ_RULE_EXECUTED] {error_message}. "
                         f"STATUS:'{STATUS_FAIL}'.")
            # return df with records failed for check with flag and
            # error message
            return (null_record_df, False, error_message, null_count)

        # If no duplicates or NULLs are found, return success
        logger.info(
            f"[DQ_RULE_EXECUTED] Rule 'Primary Key Uniqueness check' passed. "
            f"Primary key column '{primary_key_column}' contains "
            f"no duplicate values.STATUS:'{STATUS_PASS}'"
        )
        return (None, True, None, None)
    except Exception as e:
        # Handle exceptions and return error message
        error_message = (
            f"Exception occurred during primary_key_uniqueness_check(): {e}"
        )
        return ("EXCEPTION", False, error_message, None)


# Checks for duplicate records in the DataFrame.
# Returns a DataFrame of duplicate records, a success flag, and an error
# message if duplicates are found.
def duplicate_records_check(df):
    try:
        logger = get_logger()
        # Count the number of duplicate records by grouping all columns
        # and filtering
        duplicate_count = (
            df.groupBy(df.columns).count().filter(col("count") > 1).count()
        )
        if duplicate_count > 0:
            # Identify duplicate records
            duplicate_record_df = (
                df.groupBy(df.columns)
                .count()
                .filter(col("count") > 1)
                .drop("count")
            )
            # Log the error and return details
            error_message = (
                f"Rule 'Duplicate records check' "
                f"failed. Data Contains {duplicate_count} duplicate records. "
            )
            logger.error(f"[DQ_RULE_EXECUTED] {error_message}. "
                         f"STATUS:'{STATUS_FAIL}'.")
            # return df with records failed for check with flag and
            # error message
            return (duplicate_record_df, False, error_message, duplicate_count)
        # If no duplicates are found, log success and return a success flag
        logger.info(
            f"[DQ_RULE_EXECUTED] Rule 'Duplicate records check' passed. "
            f"Data contains no duplicate records. "
            f"STATUS: '{STATUS_PASS}'"
        )
        return (None, True, None, None)
    except Exception as e:
        # Handle any unexpected exceptions and return error message
        error_message = (
            f"Exception occurred during duplicate_records_check(): {e}"
        )
        return ("EXCEPTION", False, error_message, None)


# Checks for duplicate values in a specific column of the DataFrame.
# Returns a DataFrame of duplicate values, a success flag, and an error
# message if duplicates are found.
def duplicate_values_check(df, column_name):
    try:
        logger = get_logger()
        # Count the number of duplicate values in the specified column
        duplicate_count = (
            df.groupBy(column_name).count().filter(col("count") > 1).count()
        )
        if duplicate_count > 0:
            # Identify duplicate values by grouping on the column and filtering
            duplicate_record_df = (
                df.groupBy(column_name)
                .count()
                .filter(col("count") > 1)
                .drop("count")
            )
            # Fetch records from the original DataFrame that
            # have duplicate values
            duplicate_record_df = duplicate_record_df.join(
                df.distinct(), on=column_name, how="inner"
            )
            # Log the error and return details
            error_message = (
                f"Rule 'Duplicate values check' "
                f"failed. Column '{column_name}' contains {duplicate_count} "
                f"duplicate values."
            )
            logger.error(f"[DQ_RULE_EXECUTED] {error_message}. "
                         f"STATUS:'{STATUS_FAIL}'.")
            # return df with records failed for check with flag and error
            # message
            return (duplicate_record_df, False, error_message, duplicate_count)
        # If no duplicates are found, log success and return a success flag
        logger.info(
            f"[DQ_RULE_EXECUTED] Rule 'Duplicate values check' passed. "
            f"Column '{column_name}' contains no duplicate values. "
            f"STATUS:'{STATUS_PASS}'"
        )
        return (None, True, None, None)

    except Exception as e:
        # Handle any unexpected exceptions and return error message
        error_message = (
            f"Exception occurred during duplicate_values_check(): {e}"
        )
        return ("EXCEPTION", False, error_message, None)


# Checks if all values in the specified column match the expected value.
# Returns a DataFrame of invalid records, a success flag, and an error message
# if mismatches are found.
def expected_value_check(df, column_name, expected_values_list):
    try:
        logger = get_logger()
        # Convert JSON string to list
        expected_values_list = json.loads(expected_values_list)
        # Filter records where the column value is NOT in the
        # expected list or is NULL
        invalid_record_df = df.filter(
            (~col(column_name).isin(expected_values_list)) |
            (col(column_name).isNull())
        )

        invalid_count = invalid_record_df.count()
        if invalid_count > 0:
            # Log the error and return details
            error_message = (
                f"Rule 'Expected values check' failed. "
                f"Column '{column_name}' contains {invalid_count} records "
                f"with values different from the expected "
                f"value(s) '{expected_values_list}'."
            )
            logger.error(f"[DQ_RULE_EXECUTED] {error_message}. "
                         f"STATUS:'{STATUS_FAIL}'.")
            # return df with records failed for check with flag and
            # error message
            return (invalid_record_df, False, error_message, invalid_count)
        # If all values match the expected value, log success and return
        # a success flag
        logger.info(
            f"[DQ_RULE_EXECUTED] Rule 'Expected values check' passed. "
            f"Column '{column_name}' contains no records with values "
            f"different from the expected value(s) '{expected_values_list}'. "
            f"STATUS:'{STATUS_PASS}'"
        )
        return (None, True, None, None)
    except Exception as e:
        # Handle any unexpected exceptions and return error message
        error_message = (
            f"Exception occurred during expected_value_check(): {e}"
        )
        return ("EXCEPTION", False, error_message, None)


# Validates whether the specified column follows the expected date format.
# Returns a DataFrame of invalid records, a success flag, and an error message
# if any invalid formats are found.
def date_format_check(df, column_name, date_format):
    try:
        logger = get_logger()
        # Attempt to parse the date column using the provided format
        parsed_date_df = df.withColumn(
            "parsed_date", to_date(col(column_name), date_format)
        )
        # Identify records where the date parsing failed
        invalid_date_df = parsed_date_df.filter(col("parsed_date").isNull())
        invalid_count = invalid_date_df.count()
        if invalid_count > 0:
            # Log and return details if invalid date formats exist
            error_message = (
                f"Rule 'Date format check' failed. "
                f"Column '{column_name}' contains {invalid_count} records "
                f"with invalid date format."
            )
            logger.error(f"[DQ_RULE_EXECUTED] {error_message}. "
                         f"STATUS:'{STATUS_FAIL}'.")
            # return df with records failed for check with flag and
            # error message
            return (
                invalid_date_df.drop(col("parsed_date")),
                False,
                error_message,
                invalid_count
            )
        else:
            # If all dates are valid, log success and return a success flag
            logger.info(
                f"[DQ_RULE_EXECUTED] Rule 'Date format check' passed. "
                f"Column '{column_name}' contains no records with invalid "
                f"date format.STATUS:'{STATUS_PASS}'"
            )
            return (None, True, None, None)
    except Exception as e:
        # Handle unexpected exceptions and return error message
        error_message = f"Exception occurred during date_format_check(): {e}"
        return ("EXCEPTION", False, error_message, None)


# Checks if the values in the specified column meet the minimum
# value constraint.
# Returns a DataFrame of records violating the constraint, a success flag,
# and an error message if applicable.
def min_value_constraint_check(df, column_name, min_value):
    try:
        logger = get_logger()
        min_value = int(min_value)
        # Count records where the column value is less than the
        # specified minimum value
        min_value_count = df.filter(df[column_name] < min_value).count()

        if min_value_count > 0:
            # Filter records that violate the minimum value constraint
            min_value_df = df.filter(
                (df[column_name] < min_value) | (df[column_name].isNull())
            )
            # Log and return the error message along with the invalid records
            error_message = (
                f"Rule 'Minimum value constraint check' "
                f"failedn Column '{column_name}' contains {min_value_count} "
                f"records with values less than the minimum "
                f"value '{min_value}'."
            )
            logger.error(f"[DQ_RULE_EXECUTED] {error_message}. "
                         f"STATUS:'{STATUS_FAIL}'.")
            # return df with records failed for check with flag and
            # error message
            return (min_value_df, False, error_message, min_value_count)
        # If all values meet the constraint, log success and
        # return a success flag
        logger.info(
            f"[DQ_RULE_EXECUTED] Rule 'Minimum value constraint check' "
            f"passed. Column '{column_name}' contains no records with "
            f"values less than the minimum value '{min_value}'. "
            f"STATUS:'{STATUS_PASS}'"
        )
        return (None, True, None, None)

    except Exception as e:
        # Handle exceptions and return error message
        error_message = f"Exception occurred during min_value_check(): {e}"
        return ("EXCEPTION", False, error_message, None)


# Checks if the values in the specified column exceed the given maximum value.
# Returns a DataFrame of records violating the constraint, a success flag, and
# an error message if applicable.
def max_value_constraint_check(df, column_name, max_value):
    try:
        logger = get_logger()
        max_value = int(max_value)
        # Count records where the column value exceeds the
        # maximum allowed value
        max_value_count = df.filter(df[column_name] > max_value).count()
        if max_value_count > 0:
            # Filter records that violate the maximum value constraint
            max_value_df = df.filter(
                (df[column_name] > max_value) | (df[column_name].isNull())
            )
            # Log and return the error message along with the invalid records
            error_message = (
                f"Rule 'Maximum value constraint check' "
                f"failed. Column '{column_name}' contains {max_value_count} "
                f"records with values more than the maximum "
                f"value '{max_value}'."
            )
            logger.error(f"[DQ_RULE_EXECUTED] {error_message}. "
                         f"STATUS:'{STATUS_FAIL}'.")
            # return df with records failed for check with flag and
            # error message
            return (max_value_df, False, error_message, max_value_count)
        # If all values meet the constraint, log success and
        # return a success flag
        logger.info(
            f"[DQ_RULE_EXECUTED] Rule 'Maximum value constraint check' "
            f"passed. Column '{column_name}' contains no records with "
            f"values more than the maximum value '{max_value}'. "
            f"STATUS:'{STATUS_PASS}'"
        )
        return (None, True, None, None)
    except Exception as e:
        # Handle exceptions and return error message
        error_message = f"Exception occurred during min_value_check(): {e}"
        return ("EXCEPTION", False, error_message, None)


# Checks if the values in the specified column have a
# length different from the expected length.
# Returns a DataFrame of records violating the constraint,
# a success flag, and an error message if applicable.
def column_length_check(df, column_name, length_):
    try:
        logger = get_logger()
        length_ = int(length_)
        # Filter records where the column value length does not match
        # the expected length
        invalid_records_df = df.filter(length(df[column_name]) != length_)
        invalid_count = invalid_records_df.count()
        if invalid_count > 0:
            # Log and return the error message along with the invalid records
            error_message = (
                f"Rule 'Column length check' failed. "
                f"Column '{column_name}' contains {invalid_count} records "
                f"with length not equal to {length_}."
            )
            logger.error(f"[DQ_RULE_EXECUTED] {error_message}. "
                         f"STATUS:'{STATUS_FAIL}'.")
            # return df with records failed for check with flag and
            # error message
            return (invalid_records_df, False, error_message, invalid_count)

        # If all values meet the constraint, log success and return
        # a success flag
        logger.info(
            f"[DQ_RULE_EXECUTED] Rule 'Column length check' passed. "
            f"Column '{column_name}' contains no records with length "
            f"not equal to {length_}.STATUS:'{STATUS_PASS}'"
        )
        return (None, True, None, None)
    except Exception as e:
        # Handle exceptions and return error message
        error_message = f"Exception occurred during column_length_check(): {e}"
        return ("EXCEPTION", False, error_message, None)
