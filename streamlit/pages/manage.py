import pandas as pd
import streamlit as st
import time
import snowflake.snowpark.functions as F

from snowflake.snowpark.context import get_active_session

# Get the current credentials
session = get_active_session()

# ページ設定
st.set_page_config(layout="wide", page_title="データカタログ", page_icon="🧮")
st.title("Snowflake データカタログ ❄️")
st.subheader("テーブルの説明を並べ替えて更新する")

def get_dataset(table, columns=None):
    df = session.table(table)
    if columns:
        return df.select(columns)
    else:
        return df

def filter_embeddings(question):
    cmd = """
        WITH results AS (
            SELECT 
                TABLENAME, 
                DESCRIPTION, 
                CREATED_ON, 
                EMBEDDINGS, 
                VECTOR_COSINE_SIMILARITY(TABLE_CATALOG.EMBEDDINGS,
                    SNOWFLAKE.CORTEX.EMBED_TEXT_1024('voyage-multilingual-2', ?)
                ) AS similarity
            FROM TABLE_CATALOG
            ORDER BY similarity DESC
        )
        SELECT TABLENAME, DESCRIPTION, CREATED_ON 
        FROM results
    """
    
    ordered_results = session.sql(cmd, params=[question])      
    return ordered_results

descriptions_dataset = get_dataset("TABLE_CATALOG")

# テキスト入力（検索）
text_search = st.text_input(
    label="",
    placeholder="データの内容に応じてテーブルを並べ替える",
    value=""
)

if text_search and descriptions_dataset.count() > 0:
    descriptions_dataset = filter_embeddings(text_search)
    

with st.form("data_editor_form"):
    st.caption("説明を編集したい場合は手動で編集が可能です")

    if descriptions_dataset.count() == 0:
        st.write("テーブルがカタログに登録されていません。**run** ページに移動してカタログを作成してください。")
        submit_disabled = True
    else:
        # データエディタでテーブルを編集
        edited = st.data_editor(
            descriptions_dataset,
            use_container_width=True,
            disabled=['TABLENAME', 'CREATED_ON'],
            hide_index=True,
            num_rows="fixed",
            column_order=['TABLENAME', 'DESCRIPTION', 'CREATED_ON'],
            column_config={
                "TABLENAME": st.column_config.Column(
                    "テーブル名",
                    help="Snowflakeのテーブル名",
                    width=None,
                    required=True,
                ),
                "DESCRIPTION": st.column_config.Column(
                    "テーブルの説明",
                    help="LLMが生成したテーブルの説明",
                    width="large",
                    required=True,
                ),
                "CREATED_ON": st.column_config.Column(
                    "生成日",
                    help="説明が生成された日付",
                    width=None,
                    required=True,
                )
            }
        )
        submit_disabled = False

    # 送信ボタン
    submit_button = st.form_submit_button("送信", disabled=submit_disabled)

# 「送信」クリック時の処理
if submit_button:
    try:
        new_df = session.create_dataframe(edited)
        current_df = get_dataset("TABLE_CATALOG")
        _ = current_df.merge(
            new_df,
            (current_df['TABLENAME'] == new_df['TABLENAME']) &
            (current_df['DESCRIPTION'] != new_df['DESCRIPTION']),
            [
                F.when_matched().update({
                    'DESCRIPTION': new_df['DESCRIPTION'],
                    'EMBEDDINGS': F.call_udf(
                        'SNOWFLAKE.CORTEX.EMBED_TEXT_1024',
                        'voyage-multilingual-2',
                        F.col('DESCRIPTION')
                    ),
                    'CREATED_ON': F.current_timestamp()
                })
            ]
        )

        st.success("テーブルが更新されました。")
        time.sleep(5)
    except:
        st.warning("テーブルの更新中にエラーが発生しました。")

    # 成功メッセージを 5 秒間表示し、Snowflake 上の最新情報を反映するために再読み込み
    # st.experimental_rerun() # v1.38.0から廃止
    st.rerun() # v1.38.0以降