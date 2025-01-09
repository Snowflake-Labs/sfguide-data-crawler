import time
import streamlit as st
import pandas as pd

from snowflake.cortex import Complete
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.context import get_active_session

##########################################
# セッションステートの初期化
##########################################
def init_session_state():
    if "step" not in st.session_state:
        st.session_state.step = 1
    
    # もともとのコードで使用しているキー
    if "db" not in st.session_state:
        st.session_state.db = None
    if "schema" not in st.session_state:
        st.session_state.schema = None
    
    # テーブル指定関連
    if "include_tables" not in st.session_state:
        st.session_state.include_tables = []
    if "exclude_tables" not in st.session_state:
        st.session_state.exclude_tables = []
    
    # その他設定関連（元コードに合わせて）
    if "sampling_mode" not in st.session_state:
        st.session_state.sampling_mode = "fast"
    if "n" not in st.session_state:
        st.session_state.n = 5
    if "model" not in st.session_state:
        st.session_state.model = "mistral-7b"

##########################################
# モデル可用性チェック関数（元コードそのまま）
##########################################
def test_complete(session, model, prompt="Repeat the word hello once and only once. Do not say anything else.") -> bool:
    """Returns True if selected model is supported in region and returns False otherwise."""
    try:
        response = Complete(model, prompt, session=session)
        return True
    except SnowparkSQLException as e:
        if 'unknown model' in str(e):
            return False
        else:
            raise  # それ以外のエラーは再送出

##########################################
# テーブルリスト取得関数（元コードそのまま）
##########################################
def make_table_list(session, target_database, target_schema=None):
    """Returns list of selectable tables in database and, optionally schema."""
    if not target_database:
        return []
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

##########################################
# キャッシュ付きDBリスト取得（元コード）
##########################################
@st.cache_data
def get_databases(_session):
    database_result = _session.sql("SHOW DATABASES").collect()
    return [row['name'] for row in database_result]

##########################################
# スキーマリスト取得（元コード）
##########################################
def get_schemas(session):
    if st.session_state.db:
        schema_result = session.sql(f"SHOW SCHEMAS IN DATABASE {st.session_state.db}").collect()
        return [row['name'] for row in schema_result]
    else:
        return []

##########################################
# ページ設定
##########################################
st.set_page_config(layout="wide", page_title="Data Catalog Runner", page_icon="🧮")

##########################################
# Snowflakeセッション用意
##########################################
def get_snowflake_session():
    """元コードのように最初にsessionを取得。"""
    return get_active_session()

session = get_snowflake_session()

##########################################
# ステップインジケーター
##########################################
def render_step_indicator():
    steps = {
        1: "データベース選択",
        2: "テーブル選択",
        3: "設定",
        4: "確認と実行"
    }
    cols = st.columns(len(steps))
    for i, (step_number, step_name) in enumerate(steps.items(), 1):
        with cols[i-1]:
            if st.session_state.step == step_number:
                st.markdown(f"### 🔵 {i}. {step_name}")
            elif st.session_state.step > step_number:
                st.markdown(f"### ✅ {i}. {step_name}")
            else:
                st.markdown(f"### ⚪ {i}. {step_name}")

##########################################
# ステップ1: データベースとスキーマ選択
##########################################
def step1_db_and_schema():
    st.subheader("1. データベース選択")
    st.caption("Snowflake上のデータベースとオプションでスキーマを選択してください。")

    # データベース選択
    db_list = get_databases(session)
    st.session_state.db = st.selectbox("データベースを選択", options=[""] + db_list, index=0)

    # スキーマ選択
    if st.session_state.db:
        schema_list = get_schemas(session)
        st.session_state.schema = st.selectbox("スキーマ (オプション)", options=[""] + schema_list, index=0)
    
    # 次へ
    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("次へ ▶", type="primary", disabled=(not st.session_state.db)):
            st.session_state.step = 2
            st.experimental_rerun()

##########################################
# ステップ2: テーブル選択
##########################################
def step2_table_selection():
    st.subheader("2. テーブル選択")
    st.caption("処理対象に含めたいテーブルを選択、または除外テーブルを指定してください。")

    # テーブル一覧の取得
    tables = make_table_list(session, st.session_state.db, st.session_state.schema)
    if not tables:
        st.warning("テーブルがありません。前のステップに戻って再確認してください。")
        if st.button("◀ 戻る"):
            st.session_state.step = 1
            st.experimental_rerun()
        return
    
    # 「除外」かどうかのフラグ（元のコードで exclude_flag として扱う）
    exclude_flag = st.toggle("選択したテーブルを除外テーブルとして扱う", value=False)
    st.caption("チェックオフの場合は選んだテーブルのみを対象にします。オンの場合は選んだテーブルを処理から除外します。")

    # テーブルのマルチセレクト
    # format_func でスキーマ名を省略表示 (元コードに合わせて)
    split_selection = 2 if st.session_state.schema else 1
    specified_tables = st.multiselect(
        "テーブルを選択（複数可）",
        options=tables,
        format_func=lambda x: ".".join(x.split(".")[split_selection:]),
        default=[]
    )

    # 元のコードに合わせてセッションステートに反映
    st.session_state.include_tables = []
    st.session_state.exclude_tables = []
    if specified_tables:
        if exclude_flag:
            st.session_state.exclude_tables = specified_tables
        else:
            st.session_state.include_tables = specified_tables

    # 戻る・次へ
    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("◀ 戻る"):
            st.session_state.step = 1
            st.experimental_rerun()
    with col2:
        if st.button("次へ ▶", type="primary"):
            st.session_state.step = 3
            st.experimental_rerun()

##########################################
# ステップ3: 設定
##########################################
def step3_settings():
    st.subheader("3. 設定")
    st.caption("テーブルの説明を生成する際の各種オプションを設定してください。")

    # Replace catalog descriptions
    replace_catalog = st.checkbox(
        "既存のカタログ説明を再生成 (Replace catalog descriptions)",
        help="ONにすると、既存のテーブル説明を新しく生成されたテキストに置き換えます。"
    )
    # Replace table comments
    update_comment = st.checkbox(
        "テーブルコメントを更新 (Replace table comments)",
        help="ONにすると、生成されたテーブル説明でSnowflakeのテーブルコメントを上書きします。"
    )

    # サンプリングモード
    sampling_mode = st.selectbox(
        "サンプリング戦略 (Sampling strategy)",
        ("fast", "nonnull"),
        help="fast: ランダムサンプリング / nonnull: 非NULLのデータを優先的に取得"
    )

    # サンプル行数
    n = st.number_input(
        "サンプル行数 (Sample rows)",
        min_value=1, max_value=10, value=5, step=1
    )

    # モデル
    models = [
        'llama3.2-1b',
        'llama3.2-3b',
        'llama3.1-8b',
        'llama3.1-70b',
        'llama3.1-405b',
        'snowflake-arctic',
        'reka-core',
        'reka-flash',
        'mistral-large2',
        'mixtral-8x7b',
        'mistral-7b',
        'jamba-instruct',
        'jamba-1.5-mini',
        'jamba-1.5-large',
        'gemma-7b',
    ]
    model = st.selectbox(
        "Cortex LLMモデルを選択 (Cortex LLM)",
        models,
        help="テーブル説明の生成に用いるLLMを選択してください。"
    )

    # セッションステートへ反映
    st.session_state.replace_catalog = replace_catalog
    st.session_state.update_comment = update_comment
    st.session_state.sampling_mode = sampling_mode
    st.session_state.n = n
    st.session_state.model = model

    # 戻る・次へ
    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("◀ 戻る"):
            st.session_state.step = 2
            st.experimental_rerun()
    with col2:
        if st.button("次へ ▶", type="primary"):
            st.session_state.step = 4
            st.experimental_rerun()

##########################################
# ステップ4: 確認と実行 (最終的な出力は元コードと同じ)
##########################################
def step4_confirm_and_execute():
    st.subheader("4. 確認と実行")
    st.caption("指定した内容を確認して、よろしければ実行してください。")

    # --- 設定表示 ---
    st.write("**選択したデータベース・スキーマ**")
    st.write(f"- データベース: {st.session_state.db}")
    st.write(f"- スキーマ: {st.session_state.schema or '(未指定)'}")
    st.write("**テーブル指定**")
    st.write(f"include_tables: {st.session_state.include_tables}")
    st.write(f"exclude_tables: {st.session_state.exclude_tables}")

    st.write("**設定内容**")
    st.write({
        "replace_catalog": st.session_state.replace_catalog,
        "update_comment": st.session_state.update_comment,
        "sampling_mode": st.session_state.sampling_mode,
        "n_sample": st.session_state.n,
        "model": st.session_state.model,
    })

    # 戻る
    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("◀ 戻る"):
            st.session_state.step = 3
            st.experimental_rerun()

    # 実行ボタン (submit_button)
    with col2:
        submit_button = st.button("実行する (Submit)")

    # 以下、元のsubmit_buttonクリック時の処理と同じ流れ
    if submit_button:
        with st.status('モデル可用性を確認しています...') as status:
            model_available = test_complete(session, st.session_state.model)
            if model_available:
                status.update(
                    label="選択したモデルは利用可能です",
                    state="complete",
                    expanded=False
                )
            else:
                status.update(
                    label="選択したモデルはこのリージョンで利用できません。別のモデルを選んでください。",
                    state="error",
                    expanded=False
                )
        if model_available:
            with st.spinner('データをクロールし、テーブル説明を生成しています...'):
                # スキーマ未指定の場合の対処
                if not st.session_state.schema:
                    st.session_state.schema = ''
                try:
                    query = f"""
                    CALL DATA_CATALOG(
                        target_database => '{st.session_state.db}',
                        catalog_database => 'DATA_CATALOG',
                        catalog_schema => 'TABLE_CATALOG',
                        catalog_table => 'TABLE_CATALOG',
                        target_schema => '{st.session_state.schema}',
                        include_tables => {st.session_state.include_tables},
                        exclude_tables => {st.session_state.exclude_tables},
                        replace_catalog => {bool(st.session_state.replace_catalog)},
                        sampling_mode => '{st.session_state.sampling_mode}',
                        update_comment => {bool(st.session_state.update_comment)},
                        n => {int(st.session_state.n)},
                        model => '{st.session_state.model}'
                    )
                    """
                    df = session.sql(query)
                    st.dataframe(
                        df,
                        use_container_width=True,
                        hide_index=True,
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
                    st.write("**Visit 'manage' to update descriptions.**")
                except Exception as e:
                    st.warning(f"Error generating descriptions. Error: {str(e)}")




# import time
# import streamlit as st
# import pandas as pd
# from snowflake.cortex import Complete
# from snowflake.snowpark.exceptions import SnowparkSQLException
# from snowflake.snowpark.context import get_active_session

# # Get the current credentials
# session = get_active_session()
# models = [
#     'llama3.2-1b',
#     'llama3.2-3b',
#     'llama3.1-8b',
#     'llama3.1-70b',
#     'llama3.1-405b',
#     'snowflake-arctic',
#     'reka-core',
#     'reka-flash',
#     'mistral-large2',
#     'mixtral-8x7b',
#     'mistral-7b',
#     'jamba-instruct',
#     'jamba-1.5-mini',
#     'jamba-1.5-large',
#     'gemma-7b',
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