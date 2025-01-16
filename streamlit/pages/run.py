# 必要なライブラリのインポート
import time  # 時間操作用
import streamlit as st  # WebUI作成用
import pandas as pd  # データフレーム操作用
from snowflake.cortex import Complete  # Snowflake Cortex LLM機能
from snowflake.snowpark.exceptions import SnowparkSQLException  # Snowflake例外処理
from snowflake.snowpark.context import get_active_session  # Snowflakeセッション管理

# 現在のセッションを取得
session = get_active_session()

# 利用可能なLLMモデルのリスト定義
models = [
    'claude-3-5-sonnet',
    'llama3.1-8b',
    'llama3.1-70b',
    'llama3.1-405b',
    'snowflake-arctic',
    'reka-flash',
    'mistral-large2',
]

def test_complete(session, model, prompt = "Repeat the word hello once and only once. Do not say anything else.") -> bool:
    """
    指定されたモデルが現在のリージョンでサポートされているかテスト
    Args:
        session: Snowflakeセッション
        model: テストするLLMモデル名
        prompt: テスト用プロンプト
    Returns:
        bool: モデルが利用可能な場合True
    """
    try:
        response = Complete(model, prompt, session = session)
        return True
    except SnowparkSQLException as e:
        if 'unknown model' in str(e):
            return False

def make_table_list(session, target_database, target_schema = None):
    """
    データベースとスキーマから選択可能なテーブルのリストを生成
    Args:
        session: Snowflakeセッション
        target_database: 対象データベース名
        target_schema: 対象スキーマ名（オプション）
    Returns:
        list: 利用可能なテーブル名のリスト
    """
    target_schema_clause = f"AND TABLE_SCHEMA='{target_schema}'" if target_schema else ""
    query = f"""
    SELECT 
       TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME AS TABLENAME
        FROM {target_database}.INFORMATION_SCHEMA.tables 
        WHERE 1=1 
            AND TABLE_SCHEMA <> 'INFORMATION_SCHEMA' {target_schema_clause}
            AND (ROW_COUNT >= 1 OR ROW_COUNT IS NULL)
            AND IS_TEMPORARY = 'NO'
            AND NOT STARTSWITH(TABLE_NAME, '_')
    """
    table_results = session.sql(query).collect()
    tables = [row['TABLENAME'] for row in table_results]
    return tables

@st.cache_data
def get_databases(_session):
    """
    利用可能なデータベースのリストを取得（キャッシュ対応）
    Args:
        _session: Snowflakeセッション
    Returns:
        list: データベース名のリスト
    """
    database_result = _session.sql("SHOW DATABASES").collect()
    return [row['name'] for row in database_result]

def get_schemas(session):
    """
    選択されたデータベース内のスキーマリストを取得
    Args:
        session: Snowflakeセッション
    Returns:
        list: スキーマ名のリスト
    """
    if st.session_state['db']:
        schema_result = session.sql(f"SHOW SCHEMAS IN DATABASE {st.session_state['db']}").collect()
        return [row['name'] for row in schema_result]
    else:
        return []

def specify_tables(session):
    """
    テーブル選択UIコンポーネントの生成
    Args:
        session: Snowflakeセッション
    """
    with st.expander("テーブル選択（オプション）"):
        st.caption("含めるまたは除外するテーブルを指定してください。")
        if st.session_state['db']:
            split_selection = 2 if st.session_state['schema'] else 1
            selectable_tables = make_table_list(session, st.session_state['db'], st.session_state['schema'])
        else:
            selectable_tables = []
        exclude_flag = st.toggle("テーブルを除外")
        specified_tables = st.multiselect("",
                                        options = selectable_tables,
                                        format_func = lambda x: ".".join(x.split(".")[split_selection:]),
                                        default = [])
        st.session_state['include_tables'] = []
        st.session_state['exclude_tables'] = []
        if specified_tables:
            if exclude_flag:
                st.session_state['exclude_tables'] = specified_tables
            else:
                st.session_state['include_tables'] = specified_tables

# メインUIの設定
st.set_page_config(layout="wide", page_title="データカタログ生成", page_icon="🧮")
st.title("テーブルカタログ ❄️")
st.subheader("クロール対象のデータベースまたはスキーマを指定")

st.caption("クロールするSnowflakeデータを指定してください。")
d_col1, d_col2 = st.columns(2)
with d_col1:
    st.session_state['db'] = st.selectbox("データベース",
                                          options = get_databases(session),
                                          index = None,
                                          placeholder="データベースを選択")
with d_col2:
    st.session_state['schema'] = st.selectbox("スキーマ（オプション）",
                                               options = get_schemas(session),
                                               index = None,
                                               placeholder="スキーマを選択")
specify_tables(session)
st.divider()
st.caption("クロールパラメータを選択してください。")

# クロール設定オプション
replace_catalog = st.checkbox("カタログの説明を置き換え",
                            help = "選択するとテーブルの説明を再生成して置き換えます。")
update_comment = st.checkbox("テーブルコメントを更新",
                            help = "選択すると生成された説明でテーブルコメントを更新します。")
p_col1, p_col2, p_col3 = st.columns(3)
with p_col1:
    sampling_mode = st.selectbox("サンプリング戦略",
                                ("fast", "nonnull"),
                                placeholder="fast",
                                help = "fastはランダムサンプリング、nonnullは非空値を優先します。")
with p_col2:
    n = st.number_input("サンプル行数",
                       min_value = 1,
                       max_value = 10,
                       value = 5,
                       step = 1,
                       format = '%i')
with p_col3:
    model = st.selectbox("Cortex LLM",
                                models,
                                placeholder="mistral-7b",
                                help = "テーブル説明の生成に使用するLLMを選択してください。")

# 実行ボタンとプロセス処理
submit_button = st.button("実行",
                          disabled = False if st.session_state.get('db', None) else True)

if submit_button:
    # モデル利用可能性チェック
    with st.status('モデル利用可能性を確認中') as status:
        model_available = test_complete(session, model)
        if model_available:
            status.update(
            label="モデルが利用可能です", state="complete", expanded=False
        )
        else:
            status.update(
            label="このリージョンではモデルが利用できません。別のモデルを選択してください。", state="error", expanded=False
            )
    if model_available:    
        # データクロールと説明生成プロセス
        with st.spinner('データをクロール中...説明を生成中'):
            if not st.session_state['schema']:
                st.session_state['schema'] = ''
            try:
                query = f"""
                CALL DATA_CATALOG(target_database => '{st.session_state["db"]}',
                                        catalog_database => 'DATA_CATALOG',
                                        catalog_schema => 'TABLE_CATALOG',
                                        catalog_table => 'TABLE_CATALOG',
                                        target_schema => '{st.session_state["schema"]}',
                                        include_tables => {st.session_state["include_tables"]},
                                        exclude_tables => {st.session_state["exclude_tables"]},
                                        replace_catalog => {bool(replace_catalog)},
                                        sampling_mode => '{sampling_mode}', 
                                        update_comment => {bool(update_comment)},
                                        n => {int(n)},
                                        model => '{model}'
                                        )
                """
                # 結果の表示
                df = session.sql(query)
                st.dataframe(df,
                            use_container_width=True,
                            hide_index = True,
                            column_order=['TABLENAME', 'DESCRIPTION'],
                            column_config={
                "TABLENAME": st.column_config.Column(
                    "テーブル名",
                    help="Snowflakeテーブル名",
                    width=None,
                    required=True,
                ),
                "DESCRIPTION": st.column_config.Column(
                    "テーブル説明",
                    help="LLMが生成したテーブルの説明",
                    width="large",
                    required=True,
                )                   
                })
                st.write("説明を更新するには**管理**ページを参照してください。")
            except Exception as e:
                st.warning(f"説明の生成中にエラーが発生しました。エラー: {str(e)}")

# import time
# import streamlit as st
# import pandas as pd
# from snowflake.cortex import Complete
# from snowflake.snowpark.exceptions import SnowparkSQLException
# from snowflake.snowpark.context import get_active_session

# # Get the current credentials
# session = get_active_session()

# models = [
#     'claude-3-5-sonnet',
#     'llama3.1-8b',
#     'llama3.1-70b',
#     'llama3.1-405b',
#     'snowflake-arctic',
#     'reka-flash',
#     'mistral-large2',
# ]

# def test_complete(session, model, prompt = "Repeat the word hello once and only once. Do not say anything else.") -> bool:
#     """Returns True if selected model is supported in region and returns False otherwise."""
#     try:
#         response = Complete(model, prompt, session = session)
#         return True
#     except SnowparkSQLException as e:
#         if 'unknown model' in str(e):
#             return False

# def make_table_list(session,
#                     target_database,
#                     target_schema = None):
#     """Returns list of selectable tables in database and, optionally schema."""
#     target_schema_clause = f"AND TABLE_SCHEMA='{target_schema}'" if target_schema else ""
#     query = f"""
#     SELECT 
#        TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME AS TABLENAME
#         FROM {target_database}.INFORMATION_SCHEMA.tables 
#         WHERE 1=1 
#             AND TABLE_SCHEMA <> 'INFORMATION_SCHEMA' {target_schema_clause}
#             AND (ROW_COUNT >= 1 OR ROW_COUNT IS NULL)
#             AND IS_TEMPORARY = 'NO'
#             AND NOT STARTSWITH(TABLE_NAME, '_')
#     """
#     table_results = session.sql(query).collect()
#     tables = [row['TABLENAME'] for row in table_results]
#     return tables

# @st.cache_data
# def get_databases(_session):
#     database_result = _session.sql("SHOW DATABASES").collect()
#     return [row['name'] for row in database_result]

# def get_schemas(session):
#     if st.session_state['db']:
#         schema_result = session.sql(f"SHOW SCHEMAS IN DATABASE {st.session_state['db']}").collect()
#         return [row['name'] for row in schema_result]
#     else:
#         return []

# # @st.experimental_dialog("Table selection.") # Coming soon with experimental_dialog GA
# def specify_tables(session):
#     with st.expander("Table Selection (optional)"):
#         st.caption("Specify tables to include or exclude.")
#         if st.session_state['db']:
#             split_selection = 2 if st.session_state['schema'] else 1
#             selectable_tables = make_table_list(session, st.session_state['db'], st.session_state['schema'])
#         else:
#             selectable_tables = []
#         exclude_flag = st.toggle("Exclude tables")
#         specified_tables = st.multiselect("",
#                                         options = selectable_tables,
#                                         format_func = lambda x: ".".join(x.split(".")[split_selection:]),
#                                         default = [])
#         st.session_state['include_tables'] = []
#         st.session_state['exclude_tables'] = []
#         if specified_tables:
#             if exclude_flag:
#                 st.session_state['exclude_tables'] = specified_tables
#             else:
#                 st.session_state['include_tables'] = specified_tables

# st.set_page_config(layout="wide", page_title="Data Catalog Runner", page_icon="🧮")
# st.title("Catalog Tables ❄️")
# st.subheader("Specify databases or schemas to crawl")

# st.caption("Specify Snowflake data to crawl.")
# d_col1, d_col2 = st.columns(2)
# with d_col1:
#     st.session_state['db'] = st.selectbox("Database",
#                                           options = get_databases(session),
#                                           index = None,
#                                           placeholder="Select a database")
# with d_col2:
#     st.session_state['schema'] = st.selectbox("Schema (optional)",
#                                                options = get_schemas(session),
#                                                index = None,
#                                                placeholder="Select a schema")
# specify_tables(session)
# st.divider()
# st.caption("Select crawling parameters.")

# replace_catalog = st.checkbox("Replace catalog descriptions",
#                             help = "Select True to regenerate and replace table descriptions.")
# update_comment = st.checkbox("Replace table comments",
#                             help = "Select True to update table comments with generated descriptions.")
# p_col1, p_col2, p_col3 = st.columns(3)
# with p_col1:
#     sampling_mode = st.selectbox("Sampling strategy",
#                                 ("fast", "nonnull"),
#                                 placeholder="fast",
#                                 help = "Select fast to randomly sample or non-null to prioritize non-empty values.")
# with p_col2:
#     n = st.number_input("Sample rows",
#                        min_value = 1,
#                        max_value = 10,
#                        value = 5,
#                        step = 1,
#                        format = '%i')
# with p_col3:
#     model = st.selectbox("Cortex LLM",
#                                 models,
#                                 placeholder="mistral-7b",
#                                 help = "Select LLM to generate table descriptions.")

# submit_button = st.button("Submit",
#                           disabled = False if st.session_state.get('db', None) else True)

# if submit_button:
#     with st.status('Checking model availability') as status:
#         model_available = test_complete(session, model)
#         if model_available:
#             status.update(
#             label="Model available", state="complete", expanded=False
#         )
#         else:
#             status.update(
#             label="Model not available in your region. Please select another model.", state="error", expanded=False
#             )
#     if model_available:    
#         with st.spinner('Crawling data...generating descriptions'):
#             if not st.session_state['schema']: # Fix sending schema as string None
#                 st.session_state['schema'] = ''
#             try:
#                 query = f"""
#                 CALL DATA_CATALOG(target_database => '{st.session_state["db"]}',
#                                         catalog_database => 'DATA_CATALOG',
#                                         catalog_schema => 'TABLE_CATALOG',
#                                         catalog_table => 'TABLE_CATALOG',
#                                         target_schema => '{st.session_state["schema"]}',
#                                         include_tables => {st.session_state["include_tables"]},
#                                         exclude_tables => {st.session_state["exclude_tables"]},
#                                         replace_catalog => {bool(replace_catalog)},
#                                         sampling_mode => '{sampling_mode}', 
#                                         update_comment => {bool(update_comment)},
#                                         n => {int(n)},
#                                         model => '{model}'
#                                         )
#                 """
#                 df = session.sql(query)
#                 st.dataframe(df,
#                             use_container_width=True,
#                             hide_index = True,
#                             column_order=['TABLENAME', 'DESCRIPTION'],
#                             column_config={
#                 "TABLENAME": st.column_config.Column(
#                     "Table Names",
#                     help="Snowflake Table Names",
#                     width=None,
#                     required=True,
#                 ),
#                 "DESCRIPTION": st.column_config.Column(
#                     "Table Descriptions",
#                     help="LLM-generated table descriptions",
#                     width="large",
#                     required=True,
#                 )                   
#                 })
#                 # time.sleep(5)
#                 st.write("Visit **manage** to update descriptions.")
#             except Exception as e:
#                 st.warning(f"Error generating descriptions. Error: {str(e)}")