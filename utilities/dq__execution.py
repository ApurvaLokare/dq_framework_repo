from common.constants import STATUS_FAIL
from common.custom_logger import get_logger
from utilities.apply_rules import execute_data_quality_rules
from utilities.common_functions import get_active_execution_plans

"""
Executes data quality (DQ) validation by fetching the execution plan
and applying rules to the provided dataset. It categorizes the results
into critical failures, non-critical failures, exceptions, and successful rule
executions. Logs a summary of the execution and returns True if all rules pass
, otherwise returns False.
"""


def execute_data_quality_checks(
    spark, execution_plan_with_rules_df, entity_data_df, path_list,
    entity_name, batch_id
):
    try:
        logger = get_logger()
        # Fetch the execution plan as a list of rules to be applied
        execution_plans_list = get_active_execution_plans(
            execution_plan_with_rules_df
        )
        # Apply rules on the entity data
        dq_execution_result = execute_data_quality_rules(
            spark, entity_data_df, execution_plans_list, path_list,
            entity_name, batch_id
        )
        # If result is a list, count occurrences of different rule
        # validation statuses
        if isinstance(dq_execution_result, list):
            critical_failures = dq_execution_result.count(1)
            non_critical_failures = dq_execution_result.count(0)
            execution_exceptions = dq_execution_result.count(3)
            successful_checks = dq_execution_result.count(2)
            # Log the execution summary
            logger.info(
                f"[DQ_CHECK_COMPLETED] DQ Execution Summary: Critical Rules "
                f"Failed: {critical_failures}, Non-Critical Rules "
                f"Failed: {non_critical_failures}, "
                f"Exceptions:{execution_exceptions}, "
                f"Success: {successful_checks}, "
                f"STATUS:'{STATUS_FAIL}'"
            )
            # Log the process failure due to rule violations
            logger.info(
                f"[DQ_CHECK_COMPLETED] Some rules failed! Hence the "
                f"DQ process failed. STATUS:'{STATUS_FAIL}'"
            )
            return False
        # If result is a string, log the error message and fail execution
        elif isinstance(dq_execution_result, str):
            # logger.error(dq_execution_result)
            logger.info("[DQ_CHECK_COMPLETED] DQ EXECUTION FAILED!")
            return False
        # If result is a boolean, determine success or failure
        elif isinstance(dq_execution_result, bool):
            if dq_execution_result:
                logger.info(
                    "[DQ_CHECK_COMPLETED] DQ execution has been completed "
                    "successfully!"
                )
                return True
            logger.error("[DQ_CHECK_COMPLETED] DQ execution has been failed!")
            return False
    except Exception as e:
        # Handle any unexpected exceptions and log the error
        logger.error(
            f"[DQ_CHECK_COMPLETED] Exception occurred in "
            f"execute_data_quality_checks():{e}"
        )
        return False
