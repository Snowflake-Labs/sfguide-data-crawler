def run_table_catalog(session,
                      target_database, 
                      catalog_database,
                      catalog_schema,
                      catalog_table,
                      target_schema,
                      sampling_mode,
                      update_comment,
                      n,
                      model):
    
    """
    Catalogs data contained in Snowflake Database/Schema.

    Writes results to table. Output table contains
    - tablename
    - description of data contained in respective table

    Args:
        target_database (string): Snowflake database to catalog.
        catalog_database (string): Snowflake database to store table catalog.
        catalog_schema (string): Snowflake schemaname to store table catalog.
        catalog_table (string): Snowflake tablename to store table catalog.
        target_schema (string, Optional): Snowflake schema to catalog.
        sampling_mode (string): How to retrieve sample data records for table.
                                One of ['fast' (Default), 'nonnull']
                                - Pass 'fast' or omit to randomly sample records from each table.
                                - Pass 'nonnull' to prioritize least null records for table samples.
                                - Passing 'nonnull' will take considerably longer to run.
        update_comment (bool): If True, update table's current comments. Defaults to False
        n (int): Number of records to sample from table. Defaults to 5.
        model (string): Cortex model to generate table descriptions. Defaults to 'mistral-7b'.

    Returns:
        Table
    """

    import json
    import time

    import pandas as pd
    import snowflake.snowpark.functions as F

    from tables import get_crawlable_tbls, get_unique_context, get_all_tables
    from prompts import start_prompt

    tables = get_crawlable_tbls(session, target_database, target_schema, 
                                catalog_database, catalog_schema, catalog_table)
    if tables:
        context_db, context_schemas = get_unique_context(tables) # Database and set of Schemas to crawl
        schema_df = get_all_tables(session, context_db, context_schemas) # Contains all tables in schema(s)
        async_jobs = []
        for t in tables:
            current_schema = t.split('.')[1]
            prompt_args = { # Samples gathered during CATALOG_TABLE sproc
                'tablename': t,
                'table_columns': schema_df[schema_df.TABLENAME == t]['COLUMN_INFO'].to_numpy().item(),
                'table_comment': schema_df[schema_df.TABLENAME == t]['TABLE_COMMENT'].to_numpy().item(),
                'schema_tables': schema_df[schema_df.TABLE_SCHEMA == current_schema]\
                                .groupby('TABLE_SCHEMA')['TABLE_DDL']\
                                .apply(list).to_numpy().item()[0]
            }
            # Samples passed later via double {{table_samples}}
            prompt = start_prompt.format(**prompt_args).replace("'", "\\'") 
            query = f"""
            CALL DATA_CATALOG.TABLE_CATALOG.CATALOG_TABLE(
                                            tablename => '{t}',
                                            prompt => '{prompt}',
                                            sampling_mode => '{sampling_mode}',
                                            n => {n},
                                            model => '{model}',
                                            update_comment => {update_comment})
            """
            async_jobs.append(session.sql(query).collect_nowait())

        n = 20 # Give 20 seconds to finish first
        while not any(job.is_done() for job in async_jobs):
            time.sleep(n)
            n = 60 # Now check every minute
            pass

        results = [json.loads(job.result()[0][0]) for job in async_jobs if job.is_done()]
        # there is a bit of latency between when the query is done and when it is available in the query history
        while len(results) != len(tables):
            results = [json.loads(job.result()[0][0]) for job in async_jobs if job.is_done()]
            time.sleep(60)
            pass
        
        df = session.create_dataframe(pd.DataFrame.from_records(results))\
                    .withColumn('EMBEDDINGS',
                                F.call_udf('SNOWFLAKE.CORTEX.EMBED_TEXT_768',
                                           'e5-base-v2',
                                           F.col('DESCRIPTION')))
        df.write.save_as_table(table_name = [catalog_database, catalog_schema, catalog_table],
                               mode = "append",
                               column_order = "name")
        return df
    else:
        return session.create_dataframe([['No new tables to crawl','']], schema=['TABLENAME', 'DESCRIPTION'])