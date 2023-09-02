import pandas as pd
import sqlalchemy as sa
import logging

import EelConfig as ec

open_files = {}


def get_dataframe_from_sql(frame: ec.Frame, nrows=None):
    with sa.create_engine(frame.db_connection_string).connect() as sqeng:
        stmt = sa.select(sa.text("*")).select_from(sa.text(frame.sqn)).limit(nrows)
        df = pd.read_sql(stmt, con=sqeng)
    return df


def build_sql_table(df: pd.DataFrame, target: ec.Frame, add_cols: dict) -> bool:
    with sa.create_engine(target.db_connection_string).connect() as sqeng:
        sqeng.execute(sa.text(f"drop table if exists {target.sqn}"))

        # Use the first row to create the table structure
        df.head(1).to_sql(target.table, sqeng, schema=target.dbschema, index=False)

        # Delete the temporary row from the table
        sqeng.execute(sa.text(f"DELETE FROM {target.sqn}"))

        if add_cols:
            for col_name, col_val in add_cols.items():
                if col_val == ec.DynamicColumnValue.ROW_INDEX.value:
                    # Add an identity column to the table
                    sqeng.execute(
                        sa.text(
                            f"ALTER TABLE {target.sqn} ADD {col_name} int identity(1,1) PRIMARY KEY "
                        )
                    )

        sqeng.connection.commit()
    return True


def truncate_sql_table(target: ec.Target) -> bool:
    with sa.create_engine(target.db_connection_string).connect() as sqeng:
        sqeng.execute(sa.text(f"truncate table {target.sqn}"))
        sqeng.connection.commit()
    return True


def load_dataframe_to_sql(
    source_df: pd.DataFrame, target: ec.Target, add_cols: dict
) -> bool:
    with sa.create_engine(
        target.db_connection_string, fast_executemany=True
    ).connect() as sqeng:
        kwargs = target.to_sql
        if kwargs is None:
            kwargs = {}
        source_df.to_sql(
            target.table,
            sqeng,
            target.dbschema,
            index=False,
            if_exists="append",
            **kwargs,
        )
        sqeng.connection.commit()
        return True


def frames_consistent(config: ec.Config) -> bool:
    target, source, add_cols = get_configs(config)

    ignore_cols = []
    if add_cols:
        for k, v in add_cols.items():
            if v == ec.DynamicColumnValue.ROW_INDEX.value:
                ignore_cols.append(k)

    source_df = get_df(source, 100)
    source_df = add_columns(source_df, add_cols)
    target_df = get_df(target, 100)
    return data_frames_consistent(source_df, target_df, ignore_cols)


def data_frames_consistent(
    df1: pd.DataFrame, df2: pd.DataFrame, ignore_cols: list = []
) -> bool:
    res = True
    ignore_cols = set(ignore_cols)
    # Compare the column names and types
    source_cols = set(df1.columns.tolist()) - ignore_cols
    target_cols = set(df2.columns.tolist()) - ignore_cols

    if source_cols != target_cols:
        in_source = source_cols - target_cols
        in_target = target_cols - source_cols
        if in_source:
            logging.info("source has more columns:" + str(in_source))
        if in_target:
            logging.info("target has more columns:" + str(in_target))
        res = False
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
                res = False

    return res  # Table exists and has the same field names and types


# def generate_create_table_query(table_name: str, df: pd.DataFrame) -> str:
#     columns = ", ".join(
#         [f"{col} {get_sql_data_type(df[col].dtype)}" for col in df.columns]
#     )
#     query = f"CREATE TABLE {table_name} ({columns})"
#     return query


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


# def generate_insert_query(table, dataframe):
#     columns = ", ".join(dataframe.columns)
#     placeholders = ", ".join(["?" for _ in dataframe.columns])
#     query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
#     return query


def get_dataframe_from_csv(file_path, **kwargs):
    df = pd.read_csv(file_path, **kwargs)
    return df


def get_dataframe_from_excel(file_path, **kwargs):
    df = pd.read_excel(file_path, **kwargs)
    return df


def get_nrows_kwargs(source_key, nrows: int = None):
    kwargs = {}
    if source_key:
        kwargs = source_key.model_dump()
    if nrows:
        kwargs["nrows"] = nrows
    return kwargs


def get_df(frame: ec.Frame, nrows: int = None) -> pd.DataFrame:
    if frame.type in ("mssql", "postgres", "duckdb"):
        df = get_dataframe_from_sql(frame)
    elif frame.type in (".csv", ".tsv"):
        kwargs = get_nrows_kwargs(frame.read_csv, nrows)
        if frame.type == ".tsv":
            kwargs["sep"] = "\t"
        df = get_dataframe_from_csv(frame.file_path, **kwargs)
    elif frame.type in (".xlsx"):
        kwargs = get_nrows_kwargs(frame.read_excel, nrows)
        if frame.file_path in open_files:
            file = open_files[frame.file_path]
        else:
            file = frame.file_path
        df = get_dataframe_from_excel(file, **kwargs)
    return df


def put_df(df: pd.DataFrame, target: ec.Target, add_cols: dict) -> bool:
    res = False
    if df is not None:
        if not target or not target.type:
            logging.info("no target defined, printing first 100 rows:")
            print(df.head(100))
            res = True
        else:
            if target.type in (".csv"):
                df.to_csv(target.file_path, index=False)
                res = True
            elif target.type in ("mssql", "postgres", "duckdb"):
                res = load_dataframe_to_sql(df, target, add_cols)
            else:
                pass
    return res


def get_configs(config):
    target = config.target
    source = config.source
    add_cols = config.add_cols.model_dump()

    return target, source, add_cols


def add_columns(df: pd.DataFrame, add_cols: dict) -> pd.DataFrame:
    if add_cols:
        for k, v in add_cols.items():
            if (
                k != "additionalProperties"
                and v != ec.DynamicColumnValue.ROW_INDEX.value
            ):
                df[k] = v
    return df


def ingest(config: ec.Config) -> bool:
    target, source, add_cols = get_configs(config)

    if (
        not target
        or frames_consistent(config)
        or target.consistency == ec.TargetConsistencyValue.IGNORE
    ):
        source_df = get_df(source, config.nrows)
        source_df = add_columns(source_df, add_cols)
        return put_df(source_df, target, add_cols)
    else:
        logging.error(target.table + ": Inconsistent, not saved.")
        return False


def build(config: ec.Config) -> bool:
    target, source, add_cols = get_configs(config)
    if target:
        action = target.preparation_action
        if action == "create_replace":
            df = get_df(source, 100)
            df = add_columns(df, add_cols)
            res = build_sql_table(df, target, add_cols)
        elif action == "truncate":
            res = truncate_sql_table(target)
        elif action == "fail":
            logging.error("Table Exists, failing")
            res = False
        else:
            res = True
    else:
        res = True
    return res


def detect(config: ec.Config) -> bool:
    _, source, _ = get_configs(config)
    source = source.model_copy()
    source.nrows = 100

    df = get_df(source)
    print(df.dtypes.to_dict())
    return True


def write_config(config: ec.Config) -> bool:
    target, source, _ = get_configs(config)
