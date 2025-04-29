import logging
import os
from typing import Optional, Union

import numpy as np
import pandas as pd

import els.config as ec
import els.core as el
from els.io.base import ContainerReaderABC, ContainerWriterABC
from els.io.csv import CSVContainer
from els.io.fwf import FWFContainer
from els.io.pd import DFContainer
from els.io.pdf import PDFContainer
from els.io.sql import SQLContainer
from els.io.xl import XLContainer
from els.io.xml import XMLContainer


def get_container_class(
    frame: ec.Frame,
) -> type[Union[ContainerWriterABC, ContainerReaderABC]]:
    if frame.type == ".csv":
        return CSVContainer
    elif frame.type_is_excel:
        return XLContainer
    elif frame.type_is_db:
        return SQLContainer
    elif frame.type == "dict":
        return DFContainer
    elif frame.type == ".fwf":
        return FWFContainer
    elif frame.type == ".xml":
        return XMLContainer
    elif frame.type == ".pdf":
        return PDFContainer
    else:
        raise Exception(
            f"unknown {[type(frame), frame.model_dump(exclude_none=True)]} type: {frame.type}"
        )


def push_frame(
    df: pd.DataFrame,
    target: ec.Target,
    build: bool = False,
) -> bool:
    if not target or not target.type or not target.url:
        print("no target defined, printing first 100 rows:")
        print(df.head(100))
    else:
        container_class = get_container_class(target)
        df_container = el.fetch_df_container(
            container_class,
            url=target.url,
            replace=target.replace_container,
        )
        df_table = df_container.fetch_child(
            df_name=target.table,
            df=df,
        )
        df_table.set_df(
            df=df,
            if_exists=target.if_table_exists,
            build=build,
            kw_for_push=target.kw_for_push,
        )
    return True


# TODO: integrate into the container objects?
def create_directory_if_not_exists(file_path: str):
    directory = os.path.dirname(file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)


# TODO: add tests for this:
def config_frames_consistent(config: ec.Config) -> bool:
    target, source, transform = get_configs(config)

    # THIS LOGIC MAY NEED TO BE RESSURECTED
    # IT IS IGNORING IDENTITY/PRIMARY KEY FIELDS IN DATABASE,
    # ASSUMING THEY SHOULD NOT BE WRITTEN TO AND WILL NOT ALIGN WITH SOURCE
    # ignore_cols = []
    # if add_cols:
    #     for k, v in add_cols.items():
    #         if v == ec.DynamicColumnValue.ROW_INDEX.value:
    #             ignore_cols.append(k)

    source_df = pull_frame(source, sample=True)
    source_df = apply_transforms(source_df, transform, mark_as_executed=False)
    target_df = pull_frame(target, sample=True)
    return data_frames_consistent(source_df, target_df)


def apply_transforms(
    df: pd.DataFrame,
    transforms: list[ec.Transform],
    mark_as_executed: bool = True,
):
    if not transforms == [None]:
        for transform in transforms:
            if not transform.executed:
                df = transform(
                    df,
                    mark_as_executed=mark_as_executed,
                )
    return df


# CAN BE RESSURECTED FOR CSV AND EXCEL DYNAMIC CELL RESOLVING
# def get_csv_dynamic_cell_value(frame: ec.Source, add_cols):
#     # read first 10 rows of csv file with python csv reader into a list of rows
#     kwargs=frame.read_csv
#     with open(frame.url, "r", encoding="utf-8-sig") as f:
#         row_scan_max = 10
#         # get row count and update line_number for each line read
#         row_scan = sum(
#             1 for line_number, row in enumerate(f, 1) if line_number <= row_scan_max
#         )
#         f.seek(0)
#         # take min of row count and 10
#         # row_scan = 2
#         reader = csv.reader(f, delimiter=kwargs["sep"])
#         rows_n = [next(reader) for _ in range(row_scan)]
#     for k, v in add_cols.items():
#         # check if the value is a DynamicCellValue
#         if (
#             v
#             and isinstance(v, str)
#             and v[1:].upper() in ec.DynamicCellValue.__members__.keys()
#         ):
#             row, col = v[1:].upper().strip("R").split("C")
#             row = int(row)
#             col = int(col)
#             # if v == "_r1c1":
#             # get the cell value corresponding to the rxcx
#             add_cols[k] = rows_n[row][col]
#
# def get_xl_dynamic_cell_value(frame: ec.Source, add_cols):
#     for k, v in add_cols.items():
#         # check if the value is a DynamicCellValue
#         if (
#             v
#             and isinstance(v, str)
#             and v[1:].upper() in ec.DynamicCellValue.__members__.keys()
#         ):
#             row, col = v[1:].upper().strip("R").split("C")
#             row = int(row)
#             col = int(col)
#             # get the cell value corresponding to the row/col
#             add_cols[k] = xl.get_sheet_row(xl_io.file_io, frame.sheet_name, row)[col]


def data_frames_consistent(
    df1: pd.DataFrame, df2: pd.DataFrame, ignore_cols: list = []
) -> bool:
    res = True
    ignore_cols_set = set(ignore_cols)
    # Compare the column names and types
    source_cols = set(df1.columns.tolist()) - ignore_cols_set
    target_cols = set(df2.columns.tolist()) - ignore_cols_set

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
                    f"{col} has a different data type source "
                    f"{df1[col].dtype} target {df2[col].dtype}"
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


def pull_frame(
    frame: Union[ec.Source, ec.Target],
    # nrows: Optional[int] = None,
    sample: bool = False,
) -> pd.DataFrame:
    container_class = get_container_class(frame)
    df_container = el.fetch_df_container(
        container_class=container_class,
        url=frame.url,  # type: ignore
    )
    df_table = df_container[frame.table]
    df = df_table.read(
        kwargs=frame.kw_for_pull,
        sample=sample,
    )

    if frame and hasattr(frame, "dtype") and frame.dtype:
        assert df is not None
        for k, v in frame.dtype.items():
            if v == "date" and not isinstance(type(df[k]), np.dtypes.DateTime64DType):
                df[k] = pd.to_datetime(df[k])
    return pd.DataFrame(df)


def get_configs(config: ec.Config):
    target = config.target
    source = config.source
    transform = config.transform_list

    return target, source, transform


def ingest(config: ec.Config) -> bool:
    target, source, transform = get_configs(config)
    consistent = config_frames_consistent(config)
    if not target or not target.table or consistent or target.consistency == "ignore":
        # TODO: why is nrows on config root and not in source
        # this is the only place where nrows is passed to pull_frame
        source_df = pull_frame(source, False)
        source_df = apply_transforms(source_df, transform)
        return push_frame(source_df, target)
    else:
        raise Exception(f"{target.table}: Inconsistent, not saved.")


def table_exists(target: ec.Target) -> Optional[bool]:
    # TODO, bring back schema logic in new objects
    # if target.db_connection_string and target.table and target.dbschema:
    #     db_exists(target)
    #     with sa.create_engine(target.db_connection_string).connect() as sqeng:
    #         inspector = sa.inspect(sqeng)
    #         res = inspector.has_table(target.table, target.dbschema)
    assert target.url
    if target.type_is_db:
        sql_container = el.fetch_df_container(SQLContainer, target.url)
        return target.table in sql_container
    elif target.type in (".csv", ".tsv"):
        res = target.file_exists
    elif (
        target.type in (".xlsx") and target.file_exists
    ):  # TODO: add other file types supported by Calamine, be careful not to support legacy excel
        # check if sheet exists
        xl_io = el.fetch_df_container(XLContainer, target.url)
        res = target.sheet_name in xl_io
    elif target.type == "dict":
        # TODO, make these method calls consistent
        # df_dict_io = el.fetch_df_dict_io(target.url)
        df_dict_io = el.fetch_df_container(DFContainer, target.url)
        res = target.table in df_dict_io
        # TODO, the empty check may no longer be necessary if fetch is changed for get/has child
        # if target.table in df_dict and not df_dict[target.table].empty:
        #     res = True
        # else:
        #     res = False
    else:
        res = None
    return res


def requires_build_action(
    target: ec.Target,
) -> bool:
    if target.url_scheme == "file" and target.if_exists == "replace_file":
        return True
    elif target.type_is_db and target.if_exists == "replace_database":
        return True
    elif not table_exists(target) or target.if_exists == "replace":
        return True
    else:
        return False


def build(config: ec.Config) -> bool:
    target, source, transform = get_configs(config)
    if requires_build_action(target):
        # TODO, use caching to avoid pulling the same data twice
        df = pull_frame(source, sample=True)
        df = apply_transforms(df, transform, mark_as_executed=False)
        return push_frame(df, target, build=True)
    else:
        return True
