import hashlib
import importlib
import time
from datetime import datetime
from functools import reduce

from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import col, collect_list, lit

from common.constants import (EXECUTION_RESULTS_SCHEMA, RULE_MODULE,
                              STATUS_FAIL, STATUS_PASS)
from common.custom_logger import get_logger
from utilities.execution_result_saver import (save_execution_result,
                                              save_invalid_records,
                                              save_valid_records)

"""
        This function applies a list of data quality (DQ) rules
        to a given entity dataset.
        Steps:
        1. Iterates through the execution plan to extract rule details.
        2. Checks if the entity dataset is empty.
        3. Validates if the rule function exists in the
        'Rules.inbuilt_rules' module.
        4. Calls the corresponding rule function dynamically
        with necessary parameters.
        5. Captures the results:
                - If the rule passes, logs execution status.
                - If it fails, saves error records and logs the failure.
                - If an exception occurs, logs the error and continues
                to the next rule.
        6. At the end of execution:
                - If all rules pass, saves good records.
                - If any rules fail, separates good and bad records,
                saving both.
                - If all rules cause exceptions, saves the records as they are.
        Returns:
        - `True` if all rules pass.
        - A track list indicating rule outcomes (pass/fail/exception).
        - `False` if an error occurs during execution.
"""


def execute_data_quality_rules(
        spark, entity_data_df, execution_plans_list, path_list, batch_id
        ):
    try:
        logger = get_logger()
        rules_status_list = []
        failed_records_df_list = []
        output_path = path_list[1]
        error_record_path = path_list[2]
        for plan in execution_plans_list:
            var_rule_id = plan[1]
            var_column_name = plan[3]
            var_parameters = plan[4]
            var_is_critical = plan[5]
            var_rule_name = plan[7]
            var_entity_id = plan[2]
            var_plan_id = plan[0]
            var_er_id = (
                int(
                    hashlib.sha256(
                        f"{var_entity_id}-{var_plan_id}-"
                        f"{int(time.time() * 1000)}".encode()
                    ).hexdigest(),
                    16,
                )
                % 900000
                + 100000
            )

            if entity_data_df.count() == 0:
                return (
                    f"Dataframe is Empty for entity_id {var_entity_id}. "
                    f"Make sure data exists for dataframe at source for "
                    f"entity_id {var_entity_id}.Status:'{STATUS_FAIL}'"
                )
            rules_module = importlib.import_module(RULE_MODULE)
            if not hasattr(rules_module, var_rule_name):
                rules_status_list.append(3)
                logger.error(
                    f"[DQ_RULE_EXECUTION] Rule function {var_rule_name} for "
                    f"rule_id {var_rule_id} does not exists "
                    f"in {rules_module}, Skipping the rule. "
                    f"Please make sure function {var_rule_name} "
                    f"exists in module {rules_module}. STATUS:'{STATUS_FAIL}'"
                )
                continue

            execution_result = {
                "er_id": var_er_id,
                "batch_id": batch_id,
                "ep_id": int(var_plan_id),
                "rule_id": int(var_rule_id),
                "entity_id": int(var_entity_id),
                "column_name": var_column_name,
                "is_critical": var_is_critical,
                "parameter_value": var_parameters,
                "total_records": entity_data_df.count(),
                "failed_records_count": 0,
                "er_status": f"{STATUS_PASS}",
                "error_records_path": None,
                "error_message": None,
                "execution_timestamp": str(
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ),
                "year": str(datetime.now().year),
                "month": str(datetime.now().strftime("%m")),
                "day": str(datetime.now().strftime("%d")),
            }
            try:
                rule_function = getattr(rules_module, var_rule_name)

                if var_column_name and var_parameters:
                    result = rule_function(
                        entity_data_df, var_column_name, var_parameters
                    )
                elif var_column_name:
                    result = rule_function(entity_data_df, var_column_name)
                elif var_parameters:
                    result = rule_function(entity_data_df, var_parameters)
                else:
                    result = rule_function(entity_data_df)

                if result[1]:
                    rules_status_list.append(2)
                    row_data = Row(**execution_result)
                    execution_result_df = spark.createDataFrame(
                        [row_data], EXECUTION_RESULTS_SCHEMA
                    )
                    logger.info(
                        f"[DQ_RULE_EXECUTION] Saving the result, EXECUTION "
                        f"STATUS:'{STATUS_PASS}', "
                        f"EXECUTION_RESULT_ID:{var_er_id}"
                    )
                    save_execution_result(execution_result_df, var_entity_id)

                else:
                    if result[0] == "EXCEPTION":
                        rules_status_list.append(3)
                        logger.error(result[2])
                        logger.error(
                            f"[DQ_RULE_EXECUTION] Skipping the application of "
                            f"rule {var_rule_name} for rule_id {var_rule_id} "
                            f"and plan id {var_plan_id}, Please check "
                            f"{rule_function} function logs. "
                            f"STATUS:'{STATUS_FAIL}'"
                        )
                        continue

                    failed_records_df = result[0]
                    failed_rule = f"'{var_column_name}':'{var_rule_name}'"
                    failed_records_df = failed_records_df.withColumn(
                        "failed_rules_info", lit(failed_rule)
                    )

                    failed_records_df_list.append(failed_records_df)

                    execution_result["er_status"] = f"{STATUS_FAIL}"
                    execution_result["failed_records_count"] = (
                        failed_records_df.count()
                    )
                    execution_result["error_message"] = result[2]
                    execution_result["error_records_path"] = (
                        f"{error_record_path}/{batch_id}"
                    )

                    row_data = Row(**execution_result)
                    execution_result_df = spark.createDataFrame(
                        [row_data], EXECUTION_RESULTS_SCHEMA
                    )
                    logger.info(
                        f"[DQ_RULE_EXECUTION] Saving the result, "
                        f"EXECUTION STATUS:'{STATUS_FAIL}', "
                        f"EXECUTION_RESULT_ID:{var_er_id}"
                    )
                    save_execution_result(execution_result_df, var_entity_id)

                    if var_is_critical == "Y":
                        rules_status_list.append(1)
                        logger.error(
                            f"[DQ_RULE_EXECUTION] critical "
                            f"rule {var_rule_name} failed "
                            f"for entity_id {var_entity_id}."
                        )
                    else:
                        rules_status_list.append(0)
                        logger.error(
                            f"[DQ_RULE_EXECUTION] non-critical rule "
                            f"{var_rule_name} failed for entity_id "
                            f"{var_entity_id}."
                        )
            except Exception as e:
                rules_status_list.append(3)
                logger.error(
                    f"[DQ_RULE_EXECUTION] Exception occured during "
                    f"application of Rule {var_rule_name} with "
                    f"rule_id {var_rule_id}: {e}"
                )
                continue
        if rules_status_list:
            if len(set(rules_status_list)) == 1 and rules_status_list[0] == 2:
                passed_rules_count = rules_status_list.count(2)
                logger.info(
                    f"[DQ_RULE_EXECUTION]  Rules execution has been completed "
                    f"successfully! Total rules passed for "
                    f"execution:{len(rules_status_list)}, "
                    f"Total Rules Successfully Executed: {passed_rules_count} "
                    f", STATUS:'{STATUS_PASS}'"
                )
                save_valid_records(
                    entity_data_df, var_entity_id, output_path, batch_id
                )
                return True
            if failed_records_df_list:
                failed_records_df = reduce(
                    DataFrame.union,
                    [
                        df.select([col(c).cast("string") for c in df.columns])
                        for df in failed_records_df_list
                    ],
                )
                failed_records_df = failed_records_df.distinct()
                groupby_col = failed_records_df.columns[0]
                invalid_records_df = (
                    failed_records_df.groupBy(failed_records_df.columns[:-1])
                    .agg(
                        collect_list(col("failed_rules_info")).alias(
                            "failed_rules_info"
                        )
                    )
                    .orderBy(groupby_col)
                )
                critical_rule_failed = any(x == 1 for x in rules_status_list)
                if critical_rule_failed:
                    logger.error("[DQ_RULE_EXECUTION] Critical Rule(s) "
                                 "Failed. Saving Invalid records Only.")
                    save_invalid_records(
                        invalid_records_df,
                        var_entity_id,
                        error_record_path,
                        var_er_id,
                        batch_id
                    )
                    return rules_status_list
                else:
                    valid_records_df = entity_data_df.exceptAll(
                        invalid_records_df.drop("failed_rules_info")
                    ).orderBy(groupby_col)
                    logger.error("Non-Critical rule(s) failed. "
                                 "Saving the Invalid records and Valid "
                                 "records.")
                    save_invalid_records(
                        invalid_records_df,
                        var_entity_id,
                        error_record_path,
                        var_er_id,
                        batch_id
                    )
                    save_valid_records(
                        valid_records_df, var_entity_id, output_path, batch_id
                    )
                    return rules_status_list
            else:
                logger.info(
                    f"[DQ_RULE_EXECUTION] Saving the records as-is, because "
                    f"all rules encountered exceptions for entity_id "
                    f"{var_entity_id}. STATUS:'{STATUS_FAIL}'"
                )
                save_valid_records(
                    entity_data_df, var_entity_id, output_path, batch_id
                )
                return rules_status_list
        else:
            logger.error(
                f"[DQ_RULE_EXECUTION] Rules are not processed correctly, "
                f"please check output logs. STATUS:'{STATUS_FAIL}'"
            )
            return False

    except Exception as e:
        logger.error(f"[DQ_RULE_EXECUTION] Exception occured "
                     f"in apply_rules(): {e}")
        return False
