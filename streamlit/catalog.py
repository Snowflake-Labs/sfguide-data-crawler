# 必要なライブラリをインポート
import streamlit as st
from snowflake.snowpark.context import get_active_session
import plotly.express as px
import re
import ast
import pandas as pd

# ページ設定：幅広レイアウトを使用
st.set_page_config(layout="wide")

# 現在のSnowflakeセッションを取得
session = get_active_session()


# データベース一覧を取得する関数（キャッシュ付き）
@st.cache_data
def get_databases():
    # データベース一覧をSnowflakeから取得
    return pd.DataFrame({'<Select>'},columns=['DATABASE_NAME'])._append(session.sql("""
        select database_name DATABASE_NAME
        from snowflake.account_usage.databases  
        where deleted is null
        order by 1
    """).toPandas())

# テーブルカタログを取得する関数（キャッシュ付き）
@st.cache_data()
def get_table_catalog(databasename):
    df = session.sql ("SELECT distinct COMMENT, table_catalog, table_schema, table_name, table_owner, row_count FROM " + databasename + ".information_schema.tables where table_schema !='INFORMATION_SCHEMA'")
    return df.toPandas()

# テーブルの行数を取得する関数（キャッシュ付き）
@st.cache_data()
def get_count(tablename):
    df = session.sql("select count(*) as count_rows from " + tablename)
    df = df.toPandas()
    count = df['COUNT_ROWS'].values[0]
    return format(count, ',')

# カラム情報を取得する関数（キャッシュ付き）
@st.cache_data()
def get_column_data(tablename):
    df = session.sql("select TABLE_NAME, COLUMN_NAME, COMMENT from information_schema.columns where table_name= '" +tablename + "'")
    return df.toPandas()

# ユーザーアクセス情報を取得する関数（キャッシュ付き）
@st.cache_data()
def get_useraccess(tablename):
    df = session.sql("select user as USER_NAME, name as tablename from " +PARAM_TABLE_ACCESS+ " join (select distinct role, grantee_name as user from SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS) user_table on " + PARAM_TABLE_ACCESS+".GRANTEE_NAME = user_table.role where tablename='"+tablename+"';")
    return df.toPandas()

# 機密カラム情報を取得する関数（キャッシュ付き）
@st.cache_data()
def get_sensitive_column(databasename, tablename):
    df = session.sql("select COLUMN_NAME, TAG_NAME, TAG_VALUE from snowflake.account_usage.tag_references where OBJECT_DATABASE = '" + databasename + "' and OBJECT_NAME ='" + tablename + "'")
    return df.toPandas()


# Complete関数の実行
def get_response(session, prompt):
    # cortex.completeはroleがuserでないと動作しないので注意
    response = session.sql(f'''
    SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet',
        {prompt},
        {{
            'temperature': 0,
            'top_p': 0
        }});
        ''').to_pandas().iloc[0,0]
    # レスポンスを辞書型に変換
    response = ast.literal_eval(response)
    response = response["choices"][0]["messages"]
    return response
    
def get_table_context(table_name, column_data):
    context = f"""
        テーブル名は<tableName>"{str(table_name)}"</tableName>です。
        SQLのサンプルクエリはこちらです。<サンプルクエリ> select * from {str(table_name)} </サンプルクエリ> 

        また、対象のテーブルが持つ列情報は<columns>"{column_data}"</columns>です。 
    """
    return context

def get_system_prompt(table_name, column_data):
    table_context = get_table_context(table_name = table_name, column_data = column_data)
    return GEN_SQL.format(context=table_context)


GEN_SQL = """
あなたはSnowflake SQL エキスパートとして行動します。質問の回答は日本語でお願いします。
テーブルが与えられるので、テーブル名は <tableName> タグ内にあり、列は <columns> タグ内にあるので確認してください。
テーブルの概要は以下を参考にしてください。

{context}

このテーブルの概要を説明し、このテーブルの各行にあるデータにどのような相関や特徴があるかを説明してください。
また列を確認し利用可能な指標を数行で共有し、箇条書きを使用して分析例を3つを必ず挙げてください。
またなぜその分析例が効果的なのかも詳細に説明し、サンプルのSQLを生成してください。
"""


# データカタログタブの内容
st.title("データカタログ")

# アプリケーションのタイトルとサブタイトルを設定
st.subheader (f"ようこそ  :blue[{str(st.experimental_user.user_name)}] さん")

# データベース選択ドロップダウン
df_databases = get_databases()
filter_database = st.selectbox('DBを選択してください',df_databases['DATABASE_NAME'])

if not '<Select>' in filter_database:
    database_name = filter_database.split(' ')[0].replace('(','').replace(')','')
    
    with st.spinner('Retrieving the list of objects...'):
        # テーブルカタログを取得
        table_catalog = get_table_catalog(database_name)
        
        # 4列レイアウトでテーブルを表示
        col1, col2, col3, col4 = st.columns(4)
        for index, row in table_catalog.iterrows():
            # インデックスに基づいて適切な列に配置
            current_col = [col1, col2, col3, col4][index % 4]
            with current_col:
                with st.expander("**"+row['TABLE_NAME']+"**", expanded=True):
                    st.write(row['COMMENT'])
                    key_details = f"{row['TABLE_CATALOG']}.{row['TABLE_SCHEMA']}.{row['TABLE_NAME']}"
                    
                    # 詳細ボタン
                    get_data_details = st.button("詳細", key=key_details, type="primary")
                    
            
            # 詳細情報の表示
            if get_data_details:
                st.session_state.messages = []
                
                count_rows = get_count(key_details)
                table_name = ".".join(re.findall(r'[^.]+', key_details)[-1:])
                database_name = key_details.split('.')[0]

                # テーブルの概要表示
                with st.expander(str(key_details) + " の概要", expanded=True):                    
                    # 行数表示
                    st.success("レコード数 : " + str(count_rows))

                    # テーブルのPreview
                    st.info("テーブル内のカラム名と説明")
                    sql = session.sql(f"select * from {key_details} limit 10")
                    st.write(sql)
                    

                # LLM を使った分析
                with st.expander("LLMを使ったテーブルの詳細分析"):
                    column_data = get_column_data(table_name)
                    prompt = get_system_prompt(table_name, column_data)
                    st.session_state.messages.append({"role": 'user', "content": prompt})

                    # st.write(prompt)
                    response = get_response(session, st.session_state.messages)
                    st.session_state.messages.append({"role": "assistant", "content": response})
                    st.markdown(response)