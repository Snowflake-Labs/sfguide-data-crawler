CREATE DATABASE IF NOT EXISTS DATA_CATALOG
COMMENT = '{"origin": "sf_sit",
            "name": "data_catalog",
            "version": {"major": 1, "minor": 4}}';

CREATE SCHEMA IF NOT EXISTS DATA_CATALOG.TABLE_CATALOG
COMMENT = '{"origin": "sf_sit",
            "name": "data_catalog",
            "version": {"major": 1, "minor": 4}}';

CREATE OR REPLACE STAGE DATA_CATALOG.TABLE_CATALOG.SRC_FILES 
DIRECTORY = (ENABLE = true);

PUT file://src/*.py @DATA_CATALOG.TABLE_CATALOG.SRC_FILES OVERWRITE = TRUE AUTO_COMPRESS = FALSE;
PUT file://streamlit/manage.py @DATA_CATALOG.TABLE_CATALOG.SRC_FILES OVERWRITE = TRUE AUTO_COMPRESS = FALSE;
PUT file://streamlit/environment.yml @DATA_CATALOG.TABLE_CATALOG.SRC_FILES OVERWRITE = TRUE AUTO_COMPRESS = FALSE;
PUT file://streamlit/pages/run.py @DATA_CATALOG.TABLE_CATALOG.SRC_FILES/pages/ OVERWRITE = TRUE AUTO_COMPRESS = FALSE;

CREATE OR REPLACE TABLE DATA_CATALOG.TABLE_CATALOG.TABLE_CATALOG (
  TABLENAME VARCHAR
  ,DESCRIPTION VARCHAR
  ,CREATED_ON TIMESTAMP
  ,EMBEDDINGS VECTOR(FLOAT, 768)
  )
COMMENT = '{"origin": "sf_sit",
            "name": "data_catalog",
            "version": {"major": 1, "minor": 4}}';