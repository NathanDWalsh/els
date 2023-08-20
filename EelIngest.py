import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
# import pyodbc
# from joblib import Parallel, delayed
# import multiprocessing
import logging

open_files = {}

def get_db_connection_string(source):
    db_type = source["type"]
    server = source["server"]
    database = source["database"]

    # Define the connection string based on the database type
    if db_type == "mssql":
        # connection_string = f"Driver={{SQL Server}};Server={server};Database={database};Trusted_Connection=yes;"
        connection_string = f"mssql+pyodbc://localhost/bitt?driver=ODBC+Driver+17+for+SQL+Server"
    elif db_type == "postgres":
        connection_string = f"Driver={{PostgreSQL}};Server={server};Database={database};"
    elif db_type == "duckdb":
        connection_string = f"Driver={{DuckDB}};Database={database};"

    return connection_string

def get_dataframe_from_sql(config, nrows=None):
    table = config["table"]
    if 'schema' in config:
        table = config['schema'] + '.' + table

    connection_string = get_db_connection_string(config)


    # with sa.create_engine(connection_string).connect() as sqeng:
    #     Session = sessionmaker(bind=sqeng)
    #     session = Session()

    #     stmt = sa.select(sa.text('*')).select_from(sa.text(table)).limit(nrows)

    #     rows = session.execute(stmt).fetchall()

    #     df = pd.DataFrame(rows, columns=rows[0].keys())

    # return df


    with sa.create_engine(connection_string).connect() as sqeng:
        

        # Session = sessionmaker(bind=sqeng)
        # session = Session()

        stmt = sa.select(sa.text('*')).select_from(sa.text(table)).limit(nrows)

        # rows = session.execute(stmt).fetchall()

        df = pd.read_sql(stmt,  con= sqeng)

    return df



def load_dataframe_structure_to_sql(df: pd.DataFrame, target: dict, add_cols: dict, sqeng):

    # Create a new DataFrame with only one row for table creation
    df_create = df.head(1).copy()
    table_name = target["table"]

    if 'schema' in target:
        schema = target['schema']
        schema_table_name = schema + '.[' + table_name + ']'
    else:
        schema_table_name = table_name
        schema = None

    sqeng.execute(sa.text(f"drop table if exists {schema_table_name}"))
    # sqeng.connection.commit()

    # Use the first row to create the table structure
    df_create.to_sql(table_name, sqeng, schema=schema, index=False)
    # sqeng.connection.commit()

    # Delete the temporary row from the table
    sqeng.execute(sa.text(f"DELETE FROM {schema_table_name}"))
    # sqeng.connection.commit()

    row_index_col = None
    if add_cols:
        if 'row_index' in add_cols:
            row_index_col = add_cols['row_index']

    # Add an identity column to the table
    sqeng.execute(sa.text(
        f"ALTER TABLE {schema_table_name} ADD {row_index_col} int identity(1,1) PRIMARY KEY "))

    sqeng.connection.commit()


def load_dataframe_to_sql(source_df: pd.DataFrame, target: dict, add_cols: dict, build: bool):
    # conns = 'mssql+pyodbc://localhost/bi' + config['Country']['Country'] + '?driver=ODBC+Driver+17+for+SQL+Server'
    connection_string = get_db_connection_string(target)
    sqeng = sa.create_engine(
        connection_string, fast_executemany=True).connect()

    schema = target.get('schema', None)
    if_exists = target.get('if_exists', None)
    if build:
        load_dataframe_structure_to_sql(source_df, target, add_cols, sqeng)
        return True
    
    if not if_exists == 'replace':
        target_df = get_dataframe_from_sql(target,100)
        table_consistency = check_df_consistency(source_df, target_df, add_cols.values())

    if if_exists == 'replace' or table_consistency == 0 :
        load_dataframe_structure_to_sql(source_df, target, add_cols, sqeng)
        table_consistency = 1
        if_exists = 'append'
    if table_consistency >= 0 and if_exists == 'truncate':
        pass  # TODO
    if table_consistency >= 0 or (target['consistency'] == 'ignore'):
        kwargs = target.get('to_sql', {})
        source_df.to_sql(target["table"], sqeng, schema,
                  index=False, if_exists=if_exists, **kwargs)
        sqeng.connection.commit()
        sqeng.close()
        # logging.info(target["table"] + ": Data saved successfully.")
        return True
    elif table_consistency == -1:
        logging.info(target["table"] + ": Inconsistent, not saved.")
        sqeng.close()
        return False
    else:
        logging.info( target["table"] + ": something went wrong.")
        sqeng.close()
        return False
    
    

def check_df_consistency(source_df, target_df, ignore_cols=[]):
    result = 1
    ignore_cols=set(ignore_cols)
    # Compare the column names and types
    source_cols = set(source_df.columns.tolist())-ignore_cols
    target_cols = set(target_df.columns.tolist())-ignore_cols
    
    if source_cols != target_cols:
        in_source = source_cols - target_cols
        in_target = target_cols - source_cols 
        if in_source:
            logging.info('source has more columns:' + str(in_source))
        if in_target:
            logging.info('target has more columns:' + str(in_target))
        result = -1 
    else:
        for col in source_cols:
            # TODO fix this: create equivalencies between df types and possible sql types
            if target_df[col].dtype != 'object' and source_df[col].dtype != target_df[col].dtype:
                logging.info(col + ' has a different data type source ' + source_df[col].dtype +  ' target ' + target_df[col].dtype)
                result = -1  

    return result  # Table exists and has the same field names and types

def generate_create_table_query(table, dataframe):
    columns = ", ".join(
        [f"{col} {get_sql_data_type(dataframe[col].dtype)}" for col in dataframe.columns])
    query = f"CREATE TABLE {table} ({columns})"
    return query


def get_sql_data_type(dtype):
    if dtype == "int64":
        return "INT"
    elif dtype == "float64":
        return "FLOAT"
    elif dtype == "bool":
        return "BIT"
    elif dtype == "object":
        return "VARCHAR(MAX)"
    elif dtype == "datetime64":
        return "DATETIME"
    else:
        return "VARCHAR(MAX)"

def generate_insert_query(table, dataframe):
    columns = ", ".join(dataframe.columns)
    placeholders = ", ".join(["?" for _ in dataframe.columns])
    query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
    return query

def get_dataframe_from_csv(file_path, **kwargs):
    df = pd.read_csv(file_path, **kwargs)
    return df

def get_dataframe_from_excel(file_path, **kwargs):
    df = pd.read_excel(file_path, **kwargs)
    return df

def table_exists(sqeng: sa.engine, table_name: str):
    inspector = sa.inspect(sqeng)
    return inspector.has_table(table_name)

def merge_source_kwargs(source, source_key):
    kwargs = {}

    if 'nrows' in source:
        kwargs['nrows'] = source['nrows']

    # Merge source_kwargs with kwargs (source_kwargs takes precedence)
    if source_key in source:
        kwargs.update(source[source_key])

    if 'nrows' in kwargs:
        if not kwargs['nrows']:
            kwargs['nrows'] = None

    return kwargs


def get_df(config,build):
    source_type = config['type']

    if source_type in ('mssql', 'postgres', 'duckdb'):
        df = get_dataframe_from_sql(config)
    elif source_type in ('csv', 'tsv'):
        kwargs = merge_source_kwargs(config, 'read_csv')
        if build:
            kwargs['nrows'] = 100
        if source_type == 'tsv':
            kwargs['sep'] = '\t'
        df = get_dataframe_from_csv(config['file_path'], **kwargs)
    elif source_type in ('excel', 'xls', 'xlsx', 'xlsb', 'xlsm'):
        kwargs = merge_source_kwargs(config, 'read_excel')
        if build:
            kwargs['nrows'] = 100
        file_path = config['file_path']
        if file_path in open_files:
            file = open_files[file_path]
        else:
            file = file_path
        df = get_dataframe_from_excel(file, **kwargs)

    return df

def put_df(df,target,add_cols,build):
    result = False
    if df is not None:
        if target is None or not 'type' in target:
            logging.info('no target defined, printing first 100 rows:')
            print(df.head(100))
            result = True
        else:
            if target['type'] in ('csv'):
                df.to_csv(target['file_path'], index=False)
                result = True
            elif target['type'] in ('mssql', 'postgres', 'duckdb'):
                # if 'table' not in target:
                #     target['table'] = elfile_base
                result = load_dataframe_to_sql(df, target, add_cols,build)
            else:
                pass
    return result

def get_configs(config):

    target = config.get('target', None)
    source = config.get('source', {})
    add_cols = config.get('add_cols', None)

    return target, source, add_cols


def ingest(config, build):

    target, source, add_cols = get_configs(config)
    df = get_df(source,build)
    return put_df(df,target,add_cols,build)
    # print(result, source[''])
    
def detect(config):

    _, source, _ = get_configs(config)
    df = get_df(source)
    print(df.dtypes.to_dict())