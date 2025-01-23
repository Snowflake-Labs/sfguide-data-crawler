/*** 環境準備 ***/

SET (streamlit_warehouse)=(SELECT CURRENT_WAREHOUSE());

CREATE OR REPLACE DATABASE DATA_CATALOG;

CREATE SCHEMA IF NOT EXISTS DATA_CATALOG.TABLE_CATALOG;

CREATE OR REPLACE TABLE DATA_CATALOG.TABLE_CATALOG.TABLE_CATALOG (
  TABLENAME VARCHAR
  ,DESCRIPTION VARCHAR
  ,CREATED_ON TIMESTAMP
  ,EMBEDDINGS VECTOR(FLOAT, 1024)
  );

/*** マーケットプレイスデータ一覧のEmbeddingを作成 ***/
 -- Step 0: マーケットプレイスで受領したデータ一覧の確認
show available listings in data exchange snowflake_data_marketplace;

-- Step 1: 一時テーブルを作成して中間データを保存
CREATE OR REPLACE TEMPORARY TABLE temp_embedding_listings AS
WITH available_listings AS (
    SELECT * 
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
    -- WHERE "is_share_imported" = true -- 自社で取得したデータのみである場合
)
SELECT 
    PARSE_JSON("metadata"):title::STRING AS title,
    PARSE_JSON("metadata"):description::STRING AS description
FROM available_listings;

-- Step 2: 最終テーブルを作成し、埋め込みを生成
CREATE OR REPLACE TABLE marketplace_embedding_listings AS
SELECT 
    title,
    description,
    SNOWFLAKE.CORTEX.EMBED_TEXT_1024('voyage-multilingual-2', description) AS embeddings
FROM temp_embedding_listings;

-- SELECT * FROM marketplace_embedding_listings LIMIT 10;

-- オプション: 一時テーブルを削除
DROP TABLE IF EXISTS temp_embedding_listings;


/*** Git 連携 ***/
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

COPY FILES
  INTO @DATA_CATALOG.TABLE_CATALOG.SRC_FILES/pages/
  FROM @DATA_CATALOG.TABLE_CATALOG.git_data_crawler_itagaki/branches/main/streamlit/pages/
  FILES=( 'manage.py', 'run.py');
  -- FILES=( 'catalog.py', 'run.py');

-- PUT file://streamlit/manage.py @DATA_CATALOG.TABLE_CATALOG.SRC_FILES OVERWRITE = TRUE AUTO_COMPRESS = FALSE;
-- PUT file://streamlit/environment.yml @DATA_CATALOG.TABLE_CATALOG.SRC_FILES OVERWRITE = TRUE AUTO_COMPRESS = FALSE;
-- PUT file://streamlit/pages/run.py @DATA_CATALOG.TABLE_CATALOG.SRC_FILES/pages/ OVERWRITE = TRUE AUTO_COMPRESS = FALSE;

/*** ロジック作成 ***/
CREATE OR REPLACE FUNCTION DATA_CATALOG.TABLE_CATALOG.PCTG_NONNULL(records VARIANT)
returns STRING
language python
RUNTIME_VERSION = '3.10'
IMPORTS = ('@DATA_CATALOG.TABLE_CATALOG.SRC_FILES/tables.py')
HANDLER = 'tables.pctg_nonnulls'
PACKAGES = ('pandas','snowflake-snowpark-python');

CREATE OR REPLACE PROCEDURE DATA_CATALOG.TABLE_CATALOG.CATALOG_TABLE(
                                                          tablename string,
                                                          prompt string,
                                                          sampling_mode string DEFAULT 'fast', 
                                                          n integer DEFAULT 5,
                                                          model string DEFAULT 'claude-3-5-sonnet',
                                                          update_comment boolean Default FALSE)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
IMPORTS = ('@DATA_CATALOG.TABLE_CATALOG.SRC_FILES/tables.py', '@DATA_CATALOG.TABLE_CATALOG.SRC_FILES/prompts.py')
PACKAGES = ('snowflake-snowpark-python','joblib', 'pandas', 'snowflake-ml-python')
HANDLER = 'tables.generate_description'
EXECUTE AS CALLER;

CREATE OR REPLACE PROCEDURE DATA_CATALOG.TABLE_CATALOG.DATA_CATALOG(target_database string, 
                                                         catalog_database string,
                                                         catalog_schema string,
                                                         catalog_table string,
                                                         target_schema string DEFAULT '',
                                                         include_tables ARRAY DEFAULT null,
                                                         exclude_tables ARRAY DEFAULT null,
                                                         replace_catalog boolean DEFAULT TRUE,
                                                         sampling_mode string DEFAULT 'fast', 
                                                         update_comment boolean Default TRUE,
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
EXECUTE AS CALLER;

/*** SiS 作成***/
-- 2025_01バンドル以前
CREATE OR REPLACE STREAMLIT DATA_CATALOG.TABLE_CATALOG.DATA_CATALOG_APP
ROOT_LOCATION = '@data_catalog.table_catalog.src_files'
MAIN_FILE = '/catalog.py'
QUERY_WAREHOUSE = 'demo_wh';

-- 2025_01バンドル以降はDDLが変更になるので注意
-- CREATE OR REPLACE STREAMLIT DATA_CATALOG.TABLE_CATALOG.DATA_CATALOG_APP
-- FROM '@data_catalog.table_catalog.src_files'
-- MAIN_FILE = '/catalog.py'
-- QUERY_WAREHOUSE = 'demo_wh';