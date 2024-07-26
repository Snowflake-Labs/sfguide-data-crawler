CREATE OR REPLACE FUNCTION DATA_CATALOG.TABLE_CATALOG.PCTG_NONNULL(records VARIANT)
returns STRING
language python
RUNTIME_VERSION = '3.10'
IMPORTS = ('@DATA_CATALOG.TABLE_CATALOG.SRC_FILES/tables.py')
COMMENT = '{"origin": "sf_sit",
             "name": "data_catalog",
             "version": {"major": 1, "minor": 2}}'
HANDLER = 'tables.pctg_nonnulls'
PACKAGES = ('pandas','snowflake-snowpark-python');

CREATE OR REPLACE PROCEDURE DATA_CATALOG.TABLE_CATALOG.CATALOG_TABLE(
                                                          tablename string,
                                                          prompt string,
                                                          sampling_mode string DEFAULT 'fast', 
                                                          n integer DEFAULT 5,
                                                          model string DEFAULT 'mistral-7b',
                                                          update_comment boolean Default FALSE)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
IMPORTS = ('@DATA_CATALOG.TABLE_CATALOG.SRC_FILES/tables.py', '@DATA_CATALOG.TABLE_CATALOG.SRC_FILES/prompts.py')
PACKAGES = ('snowflake-snowpark-python','joblib', 'pandas', 'snowflake-ml-python')
HANDLER = 'tables.generate_description'
COMMENT = '{"origin": "sf_sit",
             "name": "data_catalog",
             "version": {"major": 1, "minor": 2}}'
EXECUTE AS CALLER;