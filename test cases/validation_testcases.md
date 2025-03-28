# Test Cases for Metadata validations

## 1️. fetch_metadata_property(metadata, table_name, property_name)

| Test Case ID | Scenario                          | Input                                                        | Expected Output                                                    | Status                                                 |
|--------------|-----------------------------------|--------------------------------------------------------------|--------------------------------------------------------------------|---------------------------------------------------------|
| TC001        | Fetch existing primary key        | ("metadata", "table_name", "primary_key")              | { "id": True }                                                     | Should return the correct primary key details           |
| TC002        | Fetch column types                | ("metadata", "table_name", "type")                       | { "rule_id": "string", "rule_name": "string" }                      | Should return column data types correctly               |
| TC003        | Fetch nullable constraints        | ("metadata", "table_name", "nullable")                | { "execution_id": False, "status": True }                            | Should correctly indicate nullable columns              |
|TC004       | Fetch foreign constraints        | ("metadata", "table_name", "foreign_key")                | {'Foreign_key column': {'table': 'Reference_table', 'column': 'Reference_column'}}                            | Should return the correct foreign key details             |
| TC005        | Table not in metadata             | ("metadata", "non_existent_table", "primary_key")            | {} with error log                                                  | Should log an error and return an empty dictionary      |
| TC006        | Property not found                | ("metadata", "table_name", "invalid_property")         | {} with error log                                                  | Should log an error and return an empty dictionary      |
| TC007        | No foreign keys exist             | ("metadata", "table_name", "foreign_key")              | {} with error log                                                  | Should return an empty dictionary                       |

---


## 2️.check_empty_dataframe(df, table_name)

| Test Case ID | Scenario                      | Input                                 | Expected Output                         | Status                                               |
|--------------|-------------------------------|---------------------------------------|-----------------------------------------|-------------------------------------------------------|
| TC008        | DataFrame contains records    | df, "table_name"       | True with success log                    | Should return True if DataFrame is not empty          |
| TC009        | Empty DataFrame               | empty_df, "table_name"           | False with error log                    | Should log an error and return False                  |

---

## 3️.validate_column_data_types(df, metadata, table_name)

| Test Case ID | Scenario                        | Input                                             | Expected Output                               | Status                                                  |
|--------------|---------------------------------|---------------------------------------------------|-----------------------------------------------|------------------------------------------------------------|
| TC010        | Matching column types           | df_correct_types, metadata, "table_name"     | True (Success log)                           | Should validate successfully if data types match           |
| TC011        | Column type mismatch            | df_wrong_types, metadata, "table_name"       | False with datatype mismatch log              | Should log an error for incorrect data types               |
| TC012        | Missing column in DataFrame     | df_missing_column, metadata, "table_name"    | False with missing column log                 | Should log an error for missing columns                    |

---

## 4️. validate_nullable_constraint(df, metadata, table_name)

| Test Case ID | Scenario                         | Input                                            | Expected Output                          | Status                                                |
|--------------|----------------------------------|--------------------------------------------------|------------------------------------------|---------------------------------------------------------|
| TC013        | No null violations               | df_no_nulls, metadata, "table_name"         | True ( Success  log)                      | Should validate successfully if no NULLs exist           |
| TC014        | Column contains NULL values      | df_with_nulls, metadata, "table_name"       | False with NULL value log                | Should log an error for NULL values                      |

---

## 5️. validate_primary_key_uniqueness(df, metadata, table_name)

| Test Case ID | Scenario                           | Input                                            | Expected Output                             | status                                                |
|--------------|------------------------------------|--------------------------------------------------|---------------------------------------------|----------------------------------------------------------|
| TC015        | Unique primary keys                | df_unique_keys, metadata, "table_name"       | True (Success log)                         | Should validate successfully if primary keys are unique  |
| TC016        | Duplicate primary key values        | df_with_duplicates, metadata, "table_name"   | False with duplicate key log                | Should log an error for duplicate keys                   |
| TC017        | Primary key column not exist        | df_missing_pk, metadata, "table_name"        | False with no PK exist log                   | Should log an error if the PK column is not exists         |

---

## 6️. validate_foreign_key_relationship(df, metadata, table_name, dfs)

| Test Case ID | Scenario                               | Input                                                        | Expected Output                                  | Status                                                |
|--------------|----------------------------------------|--------------------------------------------------------------|--------------------------------------------------|---------------------------------------------------------|
| TC018        | All foreign keys exist                 | df_with_fk, metadata, "table_name", dfs             | True ( Success log)                              | Should validate successfully if all foreign keys exist  |
| TC019        | Missing foreign key references         | df_missing_fk, metadata, "table_name", dfs             | False with missing foreign key log              | Should log an error for missing foreign key references   |
| TC020        | Parent table not found                 | df_with_fk, metadata, "table_name", missing_dfs      | False with missing parent table log             | Should log an error if the parent table is missing       |

---

## 7️. apply_validation(df, metadata, table_name)

| Test Case ID | Scenario                                | Input                                               | Expected Output                                  | Status                                                  |
|--------------|-----------------------------------------|-----------------------------------------------------|--------------------------------------------------|----------------------------------------------------------|
| TC021       | Empty DataFrame                         | empty_df, metadata, "table_name"               | False with error log (Empty DataFrame)           | Should log an error if DataFrame is empty  & skipping further validations              |
| TC022       | DataFrame  with records                      | df, metadata, "table_name"               | True          | Apply validations on dataframes   |
| TC023        | All validations pass                    | df_valid_data, metadata, "table_name"          | True (All validation logs pass)                  | Should pass if all validation are successful                |
| TC024       | Multiple validation failures            | df_multiple_issues, metadata, "table_name"       | False with multiple error logs                   | Should log multiple validation failures                 |

---

## 8️ execute_validations(validations)

| Test Case ID | Scenario                               | Input                                  | Expected Output                                   | Status                                                  |
|--------------|----------------------------------------|----------------------------------------|---------------------------------------------------|----------------------------------------------------------|
| TC025       | Execute validation for each dataframes .All validations pass                   | validations_with_correct_data          | True (Validation completed successfully)          | Should return True if all validations succeed            |
| TC026        | Some validations fail                  | validations_with_issues                | False with error logs                             | Should log failures and return False                     |

