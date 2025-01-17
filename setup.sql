SET (streamlit_warehouse)=(SELECT CURRENT_WAREHOUSE());

CREATE OR REPLACE DATABASE DATA_CATALOG;

CREATE SCHEMA IF NOT EXISTS DATA_CATALOG.TABLE_CATALOG;

-- Create API Integration for Git
CREATE OR REPLACE API INTEGRATION git_api_integration_itagaki
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/ta-taiyo')
  ENABLED = TRUE;

-- Create Git Repository
CREATE OR REPLACE GIT REPOSITORY DATA_CATALOG.TABLE_CATALOG.git_data_crawler_itagaki
  API_INTEGRATION = git_api_integration_itagaki
  ORIGIN = 'https://github.com/ta-taiyo/data-crawler-app';

ALTER GIT REPOSITORY DATA_CATALOG.TABLE_CATALOG.git_data_crawler_itagaki FETCH;

CREATE OR REPLACE STAGE DATA_CATALOG.TABLE_CATALOG.SRC_FILES 
DIRECTORY = (ENABLE = true);

COPY FILES
  INTO @DATA_CATALOG.TABLE_CATALOG.SRC_FILES/
  FROM @DATA_CATALOG.TABLE_CATALOG.git_data_crawler_itagaki/branches/main/src/
  PATTERN='.*[.]py';
-- PUT file://src/*.py @DATA_CATALOG.TABLE_CATALOG.SRC_FILES OVERWRITE = TRUE AUTO_COMPRESS = FALSE;

COPY FILES
  INTO @DATA_CATALOG.TABLE_CATALOG.SRC_FILES
  FROM @DATA_CATALOG.TABLE_CATALOG.git_data_crawler_itagaki/branches/main/streamlit/
  FILES=('catalog.py', 'environment.yml');
  -- FILES=('manage.py', 'environment.yml');

COPY FILES
  INTO @DATA_CATALOG.TABLE_CATALOG.SRC_FILES/pages/
  FROM @DATA_CATALOG.TABLE_CATALOG.git_data_crawler_itagaki/branches/main/streamlit/pages/
  FILES=( 'manage.py', 'run.py');
  -- FILES=( 'catalog.py', 'run.py');

-- PUT file://streamlit/manage.py @DATA_CATALOG.TABLE_CATALOG.SRC_FILES OVERWRITE = TRUE AUTO_COMPRESS = FALSE;
-- PUT file://streamlit/environment.yml @DATA_CATALOG.TABLE_CATALOG.SRC_FILES OVERWRITE = TRUE AUTO_COMPRESS = FALSE;
-- PUT file://streamlit/pages/run.py @DATA_CATALOG.TABLE_CATALOG.SRC_FILES/pages/ OVERWRITE = TRUE AUTO_COMPRESS = FALSE;

CREATE OR REPLACE TABLE DATA_CATALOG.TABLE_CATALOG.TABLE_CATALOG (
  TABLENAME VARCHAR
  ,DESCRIPTION VARCHAR
  ,CREATED_ON TIMESTAMP
  ,EMBEDDINGS VECTOR(FLOAT, 768)
  )
COMMENT = '{"origin": "sf_sit",
            "name": "data_catalog",
            "version": {"major": 1, "minor": 5}}';


CREATE OR REPLACE FUNCTION DATA_CATALOG.TABLE_CATALOG.PCTG_NONNULL(records VARIANT)
returns STRING
language python
RUNTIME_VERSION = '3.10'
IMPORTS = ('@DATA_CATALOG.TABLE_CATALOG.SRC_FILES/tables.py')
COMMENT = '{"origin": "sf_sit",
             "name": "data_catalog",
             "version": {"major": 1, "minor": 5}}'
HANDLER = 'tables.pctg_nonnulls'
PACKAGES = ('pandas','snowflake-snowpark-python');

CREATE OR REPLACE PROCEDURE DATA_CATALOG.TABLE_CATALOG.CATALOG_TABLE(
                                                          tablename string,
                                                          prompt string,
                                                          sampling_mode string DEFAULT 'fast', 
                                                          n integer DEFAULT 5,
                                                          model string DEFAULT 'llama3.1-70b',
                                                          update_comment boolean Default FALSE)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
IMPORTS = ('@DATA_CATALOG.TABLE_CATALOG.SRC_FILES/tables.py', '@DATA_CATALOG.TABLE_CATALOG.SRC_FILES/prompts.py')
PACKAGES = ('snowflake-snowpark-python','joblib', 'pandas', 'snowflake-ml-python')
HANDLER = 'tables.generate_description'
COMMENT = '{"origin": "sf_sit",
             "name": "data_catalog",
             "version": {"major": 1, "minor": 4}}'
EXECUTE AS CALLER;

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
             "version": {"major": 1, "minor": 5}}'
EXECUTE AS CALLER;

CREATE OR REPLACE STREAMLIT DATA_CATALOG.TABLE_CATALOG.DATA_CRAWLER
ROOT_LOCATION = '@data_catalog.table_catalog.src_files'
MAIN_FILE = '/catalog.py'
QUERY_WAREHOUSE = 'demo_wh'
COMMENT = '{"origin": "sf_sit",
            "name": "data_catalog",
            "version": {"major": 1, "minor": 5}}';