import pandas as pd
import sqlalchemy as sa
import logging
from typing import Union
import eel.config as ec

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
                            (
                                f"ALTER TABLE {target.sqn} ADD {col_name}"
                                " int identity(1,1) PRIMARY KEY "
                            )
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
            # if nulls are returned from sql and object type is set in df
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


def get_df(frame: Union[ec.Source, ec.Target], nrows: int = None) -> pd.DataFrame:
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
        # kwargs["dtype"] = {1: "str"}
        df = get_dataframe_from_excel(file, **kwargs)
    if isinstance(df.columns, pd.MultiIndex):
        if frame.stack:
            df = stack_columns(df, frame.stack)
        else:
            df = multiindex_to_singleindex(df)
    return df


def stack_columns(df, stack: ec.Stack):
    # Define the primary column headers based on the first four columns
    primary_headers = list(df.columns[: stack.fixed_columns])

    # Extract the top-level column names from the primary headers
    top_level_headers, _ = zip(*primary_headers)

    # Set the DataFrame's index to the primary headers
    df = df.set_index(primary_headers)

    # Get the names of the newly set indices
    current_index_names = list(df.index.names[: stack.fixed_columns])

    # Create a dictionary to map the current index names to the top-level headers
    index_name_mapping = dict(zip(current_index_names, top_level_headers))

    # Rename the indices using the created mapping
    df.index.rename(index_name_mapping, inplace=True)

    # Stack the DataFrame based on the top-level columns
    df = df.stack(level=stack.stack_header)

    # Rename the new index created by the stacking operation
    df.index.rename({None: stack.stack_name}, inplace=True)

    # Reset the index for the resulting DataFrame
    df.reset_index(inplace=True)

    return df


def multiindex_to_singleindex(df, separator="_"):
    df.columns = [separator.join(map(str, col)).strip() for col in df.columns.values]
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
    consistent = frames_consistent(config)
    if (
        not target
        or consistent
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