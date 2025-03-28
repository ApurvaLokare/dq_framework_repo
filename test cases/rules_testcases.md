# Test Cases for `null_check` Function

| Test Case ID | Test Scenario                          | Input DataFrame (Column)         | Expected Output                                       | Status  |
|-------------|--------------------------------------|--------------------------------|-------------------------------------------------------|---------|
| TC_001     | Column contains no null values       | Column has only non-null values | (None, True, None)                                 |  Pass |
| TC_002     | Column contains some null values     | Column has a mix of null and non-null values | (DataFrame with null records, False, Error message) |  Fail |
| TC_003     | Column contains all null values      | Column has only null values     | (DataFrame with all records, False, Error message) |  Fail |


# Test Cases for `empty_string_check` Function

| Test Case ID | Test Scenario                          | Input DataFrame (Column)         | Expected Output                                       | Status  |
|-------------|--------------------------------------|--------------------------------|-------------------------------------------------------|---------|
| TC_001     | Column contains no empty strings     | Column has only non-empty values | (None, True, None)                                 |  Pass |
| TC_002     | Column contains some empty strings   | Column has a mix of empty and non-empty values | (DataFrame with empty string records, False, Error message) |  Fail |
| TC_003     | Column contains all empty strings    | Column has only empty strings   | (DataFrame with all records, False, Error message) |  Fail |


# Test Cases for `primary_key_uniqueness_check` Function

| Test Case ID | Test Scenario                                | Input DataFrame (Primary Key Column) | Expected Output                                           | Status  |
|-------------|--------------------------------------------|--------------------------------------|-----------------------------------------------------------|---------|
| TC_001     | All values are unique, no nulls            | Unique primary key values            | (None, True, None)                                       |  Pass |
| TC_002     | Some duplicate values                      | Primary key column has duplicates    | (DataFrame with duplicate records, False, Error message) |  Fail |
| TC_003     | Some null values                           | Primary key column has nulls         | (DataFrame with null records, False, Error message)     |  Fail |
| TC_004     | Both duplicates and null values exist      | Mix of duplicate and null values     | (DataFrame with both, False, Error message)             |  Fail |
| TC_005     | All values are null                        | Entire column is null                | (DataFrame with null records, False, Error message)     |  Fail |


# Test Cases for `duplicate_records_check` Function

| Test Case ID | Test Scenario                       | Input DataFrame        | Expected Output                                                 | Status  |
|-------------|------------------------------------|----------------------|-------------------------------------------------------------|---------|
| TC_001     | No duplicate records               | Unique rows         | (None, True, None)                                         |  Pass |
| TC_002     | DataFrame contains duplicate rows  | Contains duplicates | (DataFrame with duplicates, False, Error message)         |  Fail |
| TC_003     | DataFrame has only one row         | Single row         | (None, True, None)                                         |  Pass |
| TC_004     | DataFrame contains NULL values but no duplicates | Rows with NULLs but unique | (None, True, None)  |  Pass |
| TC_005     | DataFrame contains NULL values and duplicates | Rows with NULLs and duplicates | (DataFrame with duplicates, False, Error message) |  Fail |

# Test Cases for `duplicate_values_check` Function

| Test Case ID | Test Scenario                               | Input DataFrame (Column)  | Expected Output                                                   | Status  |
|-------------|--------------------------------------------|---------------------------|-------------------------------------------------------------------|---------|
| TC_001     | No duplicate values                        | Unique values in column   | (None, True, None)                                               |  Pass |
| TC_002     | Column contains duplicate values           | Duplicates present        | (DataFrame with duplicates, False, Error message)               |  Fail |
| TC_003     | Column contains a single value (no duplicates) | Single unique value       | (None, True, None)                                               |  Pass |
| TC_004     | Column contains NULL values but no duplicates | NULLs but unique values  | (None, True, None)                                               |  Pass |
| TC_005     | Column contains NULL values and duplicates | NULLs and duplicate values | (DataFrame with duplicates, False, Error message)               |  Fail |


# Test Cases for `expected_value_check` Function

| Test Case ID | Test Scenario                                        | Input DataFrame (Column, Expected Value) | Expected Output                                                   | Status  |
|-------------|------------------------------------------------------|------------------------------------------|-------------------------------------------------------------------|---------|
| TC_001     | All values match the expected value                  | Column contains only expected values    | (None, True, None)                                               |  Pass |
| TC_002     | Some values differ from the expected value           | Column contains different values        | (DataFrame with invalid records, False, Error message)           |  Fail |
| TC_003     | No values match the expected value                   | All values are different                | (DataFrame with all records, False, Error message)               |  Fail |
| TC_004     | Column contains NULL values only                     | All values are NULL                     | (DataFrame with NULLs, False, Error message)                     |  Fail |


# Test Cases for `date_format_check` Function

| Test Case ID | Test Scenario                                        | Input DataFrame (Column, Date Format)  | Expected Output                                                   | Status  |
|-------------|------------------------------------------------------|----------------------------------------|-------------------------------------------------------------------|---------|
| TC_001     | All dates match the expected format                   | Column contains valid dates (YYYY-MM-DD) | (None, True, None)                                               |  Pass |
| TC_002     | Some dates have an incorrect format                   | Column contains both valid and invalid dates | (DataFrame with invalid dates, False, Error message)           |  Fail |
| TC_003     | All dates have an incorrect format                    | Column contains only invalid dates    | (DataFrame with all records, False, Error message)               |  Fail |
| TC_004     | Column contains only NULL values                      | All values are NULL                   | (DataFrame with NULLs, False, Error message)                     |  Fail |

# Test Cases for `min_value_constraint_check` Function

| Test Case ID | Test Scenario                                        | Input DataFrame (Column, Min Value)  | Expected Output                                                   | Status  |
|-------------|------------------------------------------------------|--------------------------------------|-------------------------------------------------------------------|---------|
| TC_001     | All values meet or exceed the minimum value          | Column contains values >= min_value  | (None, True, None)                                               |  Pass |
| TC_002     | Some values are below the minimum value              | Column contains both valid and invalid values | (DataFrame with invalid records, False, Error message)   |  Fail |
| TC_003     | All values are below the minimum value               | Column contains only values < min_value  | (DataFrame with all records, False, Error message)          |  Fail |
| TC_004     | Column contains only NULL values                     | All values are NULL                  | (None, True, None) (NULLs ignored)                              |  Pass |

# Test Cases for `max_value_constraint_check` Function

| Test Case ID | Test Scenario                                       | Input DataFrame (Column, Max Value)  | Expected Output                                                   | Status  |
|-------------|-----------------------------------------------------|--------------------------------------|-------------------------------------------------------------------|---------|
| TC_001     | All values are within the maximum limit             | Column contains values ≤ max_value   | (None, True, None)                                               |  Pass |
| TC_002     | Some values exceed the maximum limit                | Column contains both valid and invalid values | (DataFrame with invalid records, False, Error message)   |  Fail |
| TC_003     | All values exceed the maximum limit                 | Column contains only values > max_value  | (DataFrame with all records, False, Error message)          |  Fail |
| TC_004     | Column contains only NULL values                    | All values are NULL                  | (None, True, None) (NULLs ignored)                              |  Pass |

# Test Cases for `column_length_check` Function

| Test Case ID | Test Scenario                                   | Input DataFrame (Column, Expected Length)  | Expected Output                                                   | Status  |
|-------------|-----------------------------------------------|-------------------------------------------|-------------------------------------------------------------------|---------|
| TC_001     | All values have the expected length          | Column contains values with length = `length_` | (None, True, None)                                           |  Pass |
| TC_002     | Some values have incorrect lengths          | Column contains mixed valid and invalid lengths | (DataFrame with invalid records, False, Error message)   |  Fail |
| TC_003     | All values have incorrect lengths           | Column contains only values with incorrect lengths | (DataFrame with all records, False, Error message) |  Fail |