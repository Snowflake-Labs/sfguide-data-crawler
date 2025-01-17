# 必要なライブラリのインポート
import snowflake.snowpark.functions as F  # Snowflakeの関数を使用するため
import pandas as pd  # データフレーム操作のため

def get_table_comment(tablename, session):
    """
    テーブルの現在のコメントを取得する関数
    Args:
        tablename: 完全修飾テーブル名
        session: Snowflakeセッション
    Returns:
        string: テーブルコメント（シングルクォートはエスケープされる）
    """
    # テーブル名をスキーマとテーブル部分に分割
    tbl_context = tablename.split('.')
    tbl, schema = tbl_context[-1], '.'.join(tbl_context[:-1])
    return session.sql(f"SHOW TABLES LIKE '{tbl}' IN SCHEMA {schema} LIMIT 1").collect()[0]['comment'].replace("'", "\\'")

def convert_vec2array(tablename, session):
    """
    ベクトル型カラムを配列に変換する関数
    Args:
        tablename: テーブル名
        session: Snowflakeセッション
    Returns:
        DataFrame: 変換後のデータフレーム
    """
    import snowflake.snowpark.types as T

    df = session.table(tablename)
    # ベクトル型のカラムを特定
    vec_cols = [c.name for c in df.schema.fields if (type(c.datatype) == T.VectorType)]
    if vec_cols:
        # ベクトルを最初の10要素の配列に変換
        return df.select([F.array_slice(F.to_array(x), F.lit(0), F.lit(10)).as_(x) if x in vec_cols else x for x in df.columns])
    else:
        return df

def pctg_nonnulls(df):
    """
    データフレームの各行の非NULL値の割合を計算
    Args:
        df: 入力データフレーム
    Returns:
        float: 非NULL値の割合
    """
    return 1 - sum(el in [None, ''] for el in df)/len(df)

# add_records_to_catalogの詳細な説明
def add_records_to_catalog(session, catalog_database, catalog_schema, catalog_table, new_df, replace_catalog = False):
    """
    カタログデータベースにレコードを追加または更新する関数
    
    Args:
        session: Snowflakeセッション
        catalog_database: カタログ用データベース名
        catalog_schema: カタログ用スキーマ名
        catalog_table: カタログ用テーブル名
        new_df: 新しいレコードを含むデータフレーム
        replace_catalog: True=既存レコードを更新、False=新規追加のみ
    """
    if replace_catalog:
        # 既存のカタログテーブルを取得
        current_df = session.table(f'{catalog_database}.{catalog_schema}.{catalog_table}')
        
        # MERGEオペレーションを実行
        _ = current_df.merge(new_df, 
            # マージ条件：テーブル名で一致
            current_df['TABLENAME'] == new_df['TABLENAME'],
            [
                # 既存レコードの更新処理
                F.when_matched().update({
                    'DESCRIPTION': new_df['DESCRIPTION'],  # 説明を更新
                    'CREATED_ON': new_df['CREATED_ON'],    # 作成日時を更新
                    'EMBEDDINGS': F.call_udf(
                        'SNOWFLAKE.CORTEX.EMBED_TEXT_1024',  # テキスト埋め込みを生成
                        'voyage-multilingual-2',
                        new_df['DESCRIPTION']
                    )
                }),
                # 新規レコードの挿入処理
                F.when_not_matched().insert({
                    'TABLENAME': new_df['TABLENAME'],      # テーブル名
                    'DESCRIPTION': new_df['DESCRIPTION'],   # 説明
                    'CREATED_ON': new_df['CREATED_ON'],    # 作成日時
                    'EMBEDDINGS': F.call_udf(
                        'SNOWFLAKE.CORTEX.EMBED_TEXT_1024',  # テキスト埋め込みを生成
                        'voyage-multilingual-2',
                        new_df['DESCRIPTION']
                    )
                })
            ]
        )
    else:
        # 新規レコードのみを追加（APPEND）
        new_df.write.save_as_table(
            table_name = [catalog_database, catalog_schema, catalog_table],
            mode = "append",
            column_order = "name"
        )

def generate_description(session, tablename, prompt, sampling_mode, n, model, update_comment):
    """
    テーブルの説明を生成し、必要に応じてテーブルコメントを更新する関数
    Args:
        session: Snowflakeセッション
        tablename: テーブル名
        prompt: LLMに渡すプロンプト
        sampling_mode: サンプリング方法（'fast'または'nonnull'）
        n: サンプル数
        model: 使用するLLMモデル
        update_comment: テーブルコメントを更新するかどうか
    Returns:
        dict: テーブル名と生成された説明を含む辞書
    """
    response = ''
    try:
        # LLMを使用して説明を生成
        ctx_response, response = run_complete(session, tablename, model, sampling_mode, n, prompt)
        
        # コメントの更新が要求され、生成が成功した場合
        if update_comment and ctx_response == 'success':
            try:
                # テーブルのコメントを更新
                session.sql(f"COMMENT IF EXISTS ON TABLE {tablename} IS '{response}'").collect()
            except SnowparkSQLException as e:
                try:
                    # テーブルがビューの場合の処理
                    session.sql(f"COMMENT IF EXISTS ON VIEW {tablename} IS '{response}'").collect()
                except Exception as e:
                    response = f'Error encountered: {str(e)}'
            except Exception as e:
                response = f'Error encountered: {str(e)}'
    except Exception as e:
        response = f'Error encountered: {str(e)}'
    
    # 結果を返す
    return {
        'TABLENAME': tablename,
        'DESCRIPTION': response.replace("\\", "")
    }