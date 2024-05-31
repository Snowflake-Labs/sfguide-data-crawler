import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
import snowflake.snowpark.functions as F
from snowflake.snowpark.context import get_active_session

# Get the current credentials
session = get_active_session()

st.set_page_config(layout="wide", page_title="Data Catalog Runner", page_icon="🧮")
st.title("Catalog Tables ❄️")
st.subheader("Specify databases or schemas to crawl")

with st.form("submission_form"):
    st.caption("Specify Snowflake data to crawl.")
    d_col1, d_col2 = st.columns(2)
    with d_col1:
        db = st.text_input("Database", placeholder="", value="")
    with d_col2:
        schema = st.text_input("Schema", placeholder="(Optional)", value="")

    st.caption("Select crawling parameters.")
    p_col1, p_col2, p_col3, p_col4 = st.columns(4)
    with p_col1:
        sampling_mode = st.selectbox("Sampling strategy",
                                    ("fast", "nonnull"),
                                    placeholder="fast",
                                    help = "Select fast to randomly sample or non-null to prioritize non-empty values.")
    with p_col2:
        n = st.number_input("Sample rows",
                           min_value = 1,
                           max_value = 10,
                           value = 5,
                           step = 1,
                           format = '%i')
    with p_col3:
        update_comment = st.selectbox("Update table comments",
                                    options = (False, True),
                                    help = "Select True to update table comments with generated descriptions.")
    with p_col4:
        model = st.selectbox("Cortex LLM",
                                    ("mistral-7b",
                                     "llama3-8b",
                                    "snowflake-artic",
                                    "mixtral-8x7b"),
                                    placeholder="mistral-7b",
                                    help = "Select LLM to generate table descriptions.")
    
    submit_button = st.form_submit_button("Submit")

if submit_button:
    with st.spinner('Crawling data...generating descriptions'):
        try:
            query = f"""
            CALL DATA_CATALOG.TABLE_CATALOG.DATA_CATALOG(target_database => '{db}',
                                      catalog_database => 'DATA_CATALOG',
                                      catalog_schema => 'TABLE_CATALOG',
                                      catalog_table => 'TABLE_CATALOG',
                                      target_schema => '{schema}',
                                      sampling_mode => '{sampling_mode}', 
                                      update_comment => {bool(update_comment)},
                                      n => {int(n)},
                                      model => '{model}'
                                      )
            """
            df = session.sql(query)
            st.dataframe(df,
                        use_container_width=True,
                        hide_index = True,
                        column_order=['TABLENAME', 'DESCRIPTION'],
                        column_config={
            "TABLENAME": st.column_config.Column(
                "Table Names",
                help="Snowflake Table Names",
                width=None,
                required=True,
            ),
            "DESCRIPTION": st.column_config.Column(
                "Table Descriptions",
                help="LLM-generated table descriptions",
                width="large",
                required=True,
            )                   
            })
            # time.sleep(5)
            st.write("Visit **manage** to update descriptions.")
        except:
            st.warning("Error generating descriptions.")