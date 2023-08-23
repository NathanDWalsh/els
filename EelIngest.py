import pandas as pd
import sqlalchemy as sa
import logging

import EelConfig as ec

open_files = {}


def get_db_connection_string(source: ec.Target) -> str:
    db_type = source.type
    server = source.server
    database = source.database

    # Define the connection string based on the database type
    if db_type == "mssql":
        # connection_string = f"Driver={{SQL Server}};Server={server};Database={database};Trusted_Connection=yes;"
        connection_string = (
            f"mssql+pyodbc://localhost/bitt?driver=ODBC+Driver+17+for+SQL+Server"
        )
    elif db_type == "postgres":
        connection_string = (
            f"Driver={{PostgreSQL}};Server={server};Database={database};"
        )
    elif db_type == "duckdb":
        connection_string = f"Driver={{DuckDB}};Database={database};"

    return connection_string


def get_dataframe_from_sql(config: ec.Target, nrows=None):
    table = config.sqn

    connection_string = get_db_connection_string(config)

    with sa.create_engine(connection_string).connect() as sqeng:
        stmt = sa.select(sa.text("*")).select_from(sa.text(table)).limit(nrows)
        df = pd.read_sql(stmt, con=sqeng)
    return df


def build_dataframe_to_sql(
    df: pd.DataFrame, target: ec.Target, add_cols: ec.AddColumns
) -> bool:
    connection_string = get_db_connection_string(target)
    sqeng = sa.create_engine(connection_string, fast_executemany=True).connect()

    # Create a new DataFrame with only one row for table creation
    df_create = df.head(1).copy()
    table_name = target.table

    schema = target.dbschema
    schema_table_name = target.sqn

    sqeng.execute(sa.text(f"drop table if exists {schema_table_name}"))
    # sqeng.connection.commit()

    # Use the first row to create the table structure
    df_create.to_sql(table_name, sqeng, schema=schema, index=False)
    # sqeng.connection.commit()

    # Delete the temporary row from the table
    sqeng.execute(sa.text(f"DELETE FROM {schema_table_name}"))
    # sqeng.connection.commit()

    if add_cols and add_cols.row_index:
        # Add an identity column to the table
        sqeng.execute(
            sa.text(
                f"ALTER TABLE {schema_table_name} ADD {add_cols.row_index} int identity(1,1) PRIMARY KEY "
            )
        )

    sqeng.connection.commit()
    return True


def load_dataframe_to_sql(
    source_df: pd.DataFrame, target: ec.Target, add_cols: ec.AddColumns
) -> bool:
    # conns = 'mssql+pyodbc://localhost/bi' + config['Country']['Country'] + '?driver=ODBC+Driver+17+for+SQL+Server'
    connection_string = get_db_connection_string(target)
    sqeng = sa.create_engine(connection_string, fast_executemany=True).connect()

    schema = target.dbschema

    if table_exists(sqeng, target.sqn):
        target_df = get_dataframe_from_sql(target, 100)
        tables_consistent = data_frames_consistent(
            source_df, target_df, add_cols.model_dump().values()
        )
    else:
        tables_consistent = False

    # if table_consistency == 0 :
    #     load_dataframe_structure_to_sql(source_df, target, add_cols, sqeng)
    #     table_consistency = 1
    #     if_exists = 'append'
    # if table_consistency >= 0 and if_exists == 'truncate':
    #     pass  # TODO
    if (
        tables_consistent
        or target.consistency == "ignore"
        or target.if_exists == "replace"
    ):
        kwargs = target.to_sql
        if kwargs is None:
            kwargs = {}
        source_df.to_sql(
            target.table, sqeng, schema, index=False, if_exists="append", **kwargs
        )
        sqeng.connection.commit()
        sqeng.close()
        # logging.info(target["table"] + ": Data saved successfully.")
        return True
    elif not tables_consistent:
        logging.info(target.table + ": Inconsistent, not saved.")
        sqeng.close()
        return False
    else:
        logging.info(target.table + ": something went wrong.")
        sqeng.close()
        return False


def data_frames_consistent(
    df1: pd.DataFrame, df2: pd.DataFrame, ignore_cols: list = []
) -> bool:
    result = True
    ignore_cols = set(ignore_cols)
    # Compare the column names and types
    source_cols = set(df1.columns.tolist()) - ignore_cols
    target_cols = set(df2.columns.tolist()) - ignore_cols

    if source_cols != target_cols:
        in_source = source_cols - target_cols
        in_target = target_cols - source_cols
        if in_source:
            logging.info("df1 has more columns:" + str(in_source))
        if in_target:
            logging.info("df2 has more columns:" + str(in_target))
        result = False
    else:
        for col in source_cols:
            # TODO fix this: create equivalencies between df types and possible sql types
            if df2[col].dtype != "object" and df1[col].dtype != df2[col].dtype:
                logging.info(
                    col
                    + " has a different data type source "
                    + df1[col].dtype
                    + " target "
                    + df2[col].dtype
                )
                result = False

    return result  # Table exists and has the same field names and types


def generate_create_table_query(table_name: str, df: pd.DataFrame) -> str:
    columns = ", ".join(
        [f"{col} {get_sql_data_type(df[col].dtype)}" for col in df.columns]
    )
    query = f"CREATE TABLE {table_name} ({columns})"
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


def merge_source_kwargs(source: ec.Source, source_key):
    kwargs = {}

    if source.nrows:
        kwargs["nrows"] = source.nrows

    # Merge source_kwargs with kwargs (source_kwargs takes precedence)
    if source_key:
        kwargs.update(source_key.model_dump())

    if "nrows" in kwargs:
        if not kwargs["nrows"]:
            kwargs["nrows"] = None

    return kwargs


def get_df(config: ec.Source) -> pd.DataFrame:
    if config.type in ("mssql", "postgres", "duckdb"):
        df = get_dataframe_from_sql(config)
    elif config.type in ("csv", "tsv"):
        kwargs = merge_source_kwargs(config, config.read_csv)
        if config.type == "tsv":
            kwargs["sep"] = "\t"
        df = get_dataframe_from_csv(config.file_path, **kwargs)
    elif config.type in ("excel", "xls", "xlsx", "xlsb", "xlsm"):
        kwargs = merge_source_kwargs(config, config.read_excel)
        if config.file_path in open_files:
            file = open_files[config.file_path]
        else:
            file = config.file_path
        df = get_dataframe_from_excel(file, **kwargs)

    return df


def put_df(df: pd.DataFrame, target: ec.Target, add_cols: ec.AddColumns) -> bool:
    result = False
    if df is not None:
        if not target or not target.type:
            logging.info("no target defined, printing first 100 rows:")
            print(df.head(100))
            result = True
        else:
            if target.type in ("csv"):
                df.to_csv(target.file_path, index=False)
                result = True
            elif target.type in ("mssql", "postgres", "duckdb"):
                # if 'table' not in target:
                #     target['table'] = elfile_base
                result = load_dataframe_to_sql(df, target, add_cols)
            else:
                pass
    return result


def get_configs(config):
    target = config.target
    source = config.source
    add_cols = config.add_cols

    return target, source, add_cols


def ingest(config: ec.Config) -> bool:
    target, source, add_cols = get_configs(config)
    df = get_df(source)
    return put_df(df, target, add_cols)
    # print(result, source[''])


def build(config: ec.Config) -> bool:
    target, source, add_cols = get_configs(config)
    source = source.model_copy()
    source.nrows = 100
    df = get_df(source)
    res = build_dataframe_to_sql(df, target, add_cols)
    return res


def detect(config: ec.Config):
    _, source, _ = get_configs(config)
    df = get_df(source)
    print(df.dtypes.to_dict())
