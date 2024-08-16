CREATE OR REPLACE PROCEDURE DATA_CATALOG.TABLE_CATALOG.DATA_CATALOG(target_database string, 
                                                         catalog_database string,
                                                         catalog_schema string,
                                                         catalog_table string,
                                                         target_schema string DEFAULT '',
                                                         include_tables ARRAY DEFAULT null,
                                                         exclude_tables ARRAY DEFAULT null,
                                                         replace_catalog boolean DEFAULT FALSE,
                                                         sampling_mode string DEFAULT 'fast', 
                                                         update_comment boolean Default FALSE,
                                                         n integer DEFAULT 5,
                                                         model string DEFAULT 'mistral-7b'
                                                         )
RETURNS TABLE()
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python','pandas', 'snowflake-ml-python')
IMPORTS = ('@DATA_CATALOG.TABLE_CATALOG.SRC_FILES/tables.py',
           '@DATA_CATALOG.TABLE_CATALOG.SRC_FILES/main.py',
           '@DATA_CATALOG.TABLE_CATALOG.SRC_FILES/prompts.py')
HANDLER = 'main.run_table_catalog'
COMMENT = '{"origin": "sf_sit",
             "name": "data_catalog",
             "version": {"major": 1, "minor": 3}}'
EXECUTE AS CALLER;

-- EXAMPLE RUN:
-- CALL DATA_CATALOG.TABLE_CATALOG.DATA_CATALOG(target_database => 'JSUMMER',
--                                   catalog_database => 'DATA_CATALOG',
--                                   catalog_schema => 'TABLE_CATALOG',
--                                   catalog_table => 'TABLE_CATALOG',
--                                   target_schema => 'CATALOG',
--                                   sampling_mode => 'fast', 
--                                   update_comment => TRUE
--                                   );
