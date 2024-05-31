import pandas as pd
import streamlit as st
import time
import snowflake.snowpark.functions as F

from snowflake.snowpark.context import get_active_session

# Get the current credentials
session = get_active_session()

st.set_page_config(layout="wide", page_title="Data Catalog", page_icon="🧮")
st.title("Snowflake Data Catalog ❄️")
st.subheader("Sort and update table descriptions")

def get_dataset(table, columns = None):
    df = session.table(table)
    if columns:
        return df.select(columns)
    else:
        return df

def filter_embeddings(question):

    cmd = """
        with results as
        (SELECT TABLENAME, DESCRIPTION, EMBEDDINGS, 
           VECTOR_COSINE_SIMILARITY(TABLE_CATALOG.EMBEDDINGS,
                    SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', ?)) as similarity
        from TABLE_CATALOG
        order by similarity desc)
        select TABLENAME, DESCRIPTION from results 
    """
    
    ordered_results = session.sql(cmd, params=[question])      
             
    return ordered_results

descriptions_dataset = get_dataset("TABLE_CATALOG")

text_search = st.text_input("", placeholder="Sort tables by data context", value="")

if text_search and descriptions_dataset.count() > 0:             
    descriptions_dataset = filter_embeddings(text_search)

with st.form("data_editor_form"):
    st.caption("Edit the descriptions below")
    if descriptions_dataset.count() == 0:
        st.write("No tables catalogued. Visit **run** page to catalog.")
        submit_disabled = True
    else:
        edited = st.data_editor(descriptions_dataset,
                                use_container_width=True,
                                disabled=['TABLENAME'],
                                hide_index = True,
                                num_rows = "fixed",
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
            }
            )
        submit_disabled = False
    submit_button = st.form_submit_button("Submit", disabled = submit_disabled)


if submit_button:
    try:
        full_dataset = session.create_dataframe(edited)\
                           .withColumn('EMBEDDINGS',
                                F.call_udf('SNOWFLAKE.CORTEX.EMBED_TEXT_768',
                                           'e5-base-v2',
                                           F.col('DESCRIPTION')))
        full_dataset.write.save_as_table(table_name = "TABLE_CATALOG",
                                         mode = "overwrite",
                                         column_order = "name") # TODO FIND FIX FOR THIS BUG
        st.success("Table updated")
        time.sleep(5)
    except:
        st.warning("Error updating table")
    #display success message for 5 seconds and update the table to reflect what is in Snowflake
    st.experimental_rerun()