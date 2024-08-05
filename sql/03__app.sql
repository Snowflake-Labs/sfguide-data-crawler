CREATE OR REPLACE STREAMLIT DATA_CATALOG.TABLE_CATALOG.DATA_CRAWLER
ROOT_LOCATION = '@data_catalog.table_catalog.src_files'
MAIN_FILE = '/manage.py'
QUERY_WAREHOUSE = WH_XS
COMMENT = '{"origin": "sf_sit",
            "name": "data_catalog",
            "version": {"major": 1, "minor": 3}}';