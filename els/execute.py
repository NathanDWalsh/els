import io
import logging
import os
from typing import Optional, Union

import duckdb
import numpy as np
import pandas as pd
import prqlc
from pdfminer.high_level import LAParams, extract_pages
from pdfminer.layout import LTChar, LTTextBox

import els.config as ec
import els.core as el
import els.io.csv as csv
import els.io.pd as pn
import els.io.sql as sq
import els.io.xl as xl


def push_frame(
    df: pd.DataFrame,
    target: ec.Target,
    build: bool = False,
) -> bool:
    if not target or not target.type:
        print("no target defined, printing first 100 rows:")
        print(df.head(100))
    else:
        if target.type in (".csv"):
            container_class = csv.CSVIO
        elif target.type in (".xlsx"):
            container_class = xl.ExcelIO
        elif target.type_is_db:
            container_class = sq.SQLDBContainer
        elif target.type in ("dict"):
            container_class = pn.DataFrameDictIO
        else:
            raise Exception(f"unknown target type: {target.type}")
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


def apply_transforms(df, transform, mark_as_executed: bool = True):
    if not transform == [None]:
        for tx in transform:
            if not tx.executed:
                if isinstance(tx, ec.FilterTransform):
                    df = df.query(tx.filter)

                elif isinstance(tx, ec.PrqlTransform):
                    if os.path.isfile(tx.prql):
                        with io.open(tx.prql) as file:
                            prql = file.read()
                    else:
                        prql = tx.prql
                    prqlo = prqlc.CompileOptions(target="sql.duckdb")
                    dsql = prqlc.compile(prql, options=prqlo)
                    df = duckdb.sql(dsql).df()

                elif isinstance(tx, ec.SplitOnColumn):
                    # this transform is converted to a filter transform in the path module
                    # it may need to moved here
                    # should be here in the case of multiple splits, the path one only considers the final one
                    # for now multiple splits are not supported
                    # raise Exception("Multiple splits are not supported")
                    pass

                elif isinstance(tx, ec.Pivot):
                    df = df.pivot(
                        columns=tx.pivot_columns,
                        values=tx.pivot_values,
                        index=tx.pivot_index,
                    )
                    df.columns.name = None
                    df.index.name = None

                elif isinstance(tx, ec.AsType):
                    df = df.astype(tx.as_dtypes)

                elif isinstance(tx, ec.Melt):
                    df = pd.melt(
                        df,
                        id_vars=tx.melt_id_vars,
                        value_vars=tx.melt_value_vars,
                        value_name=tx.melt_value_name,
                        var_name=tx.melt_var_name,
                    )

                elif isinstance(tx, ec.StackDynamic):
                    df = stack_columns(df, tx)

                elif isinstance(tx, ec.AddColumns):
                    add_columns(df, tx.model_dump(exclude="additionalProperties"))
                tx.executed = mark_as_executed
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


def text_range_to_list(text):
    result = []
    segments = text.split(",")
    for segment in segments:
        if "-" in segment:
            start, end = map(int, segment.split("-"))
            result.extend(range(start, end + 1))
        else:
            result.append(int(segment))
    return result


def clean_page_numbers(page_numbers):
    if isinstance(page_numbers, int):
        res = [page_numbers]
    if isinstance(page_numbers, str):
        res = text_range_to_list(page_numbers)
    else:
        res = page_numbers
    return sorted(res)


def pull_pdf(file, laparams, **kwargs) -> pd.DataFrame:
    def get_first_char_from_text_box(tb) -> LTChar:
        for line in tb:
            for char in line:
                return char

    lap = LAParams()
    if laparams:
        for k, v in laparams.items():
            lap.__setattr__(k, v)

    if "page_numbers" in kwargs:
        kwargs["page_numbers"] = clean_page_numbers(kwargs["page_numbers"])

    pm_pages = extract_pages(file, laparams=lap, **kwargs)

    dict_res = {
        "page_index": [],
        "y0": [],
        "y1": [],
        "x0": [],
        "x1": [],
        "height": [],
        "width": [],
        "font_name": [],
        "font_size": [],
        "font_color": [],
        "text": [],
    }

    for p in pm_pages:
        for e in p:
            if isinstance(e, LTTextBox):
                first_char = get_first_char_from_text_box(e)
                dict_res["page_index"].append(
                    kwargs["page_numbers"][p.pageid - 1]
                    if "page_numbers" in kwargs
                    else p.pageid
                )
                dict_res["x0"].append(e.x0)
                dict_res["x1"].append(e.x1)
                dict_res["y0"].append(e.y0)
                dict_res["y1"].append(e.y1)
                dict_res["height"].append(e.height)
                dict_res["width"].append(e.width)
                dict_res["font_name"].append(first_char.fontname)
                dict_res["font_size"].append(first_char.height)
                dict_res["font_color"].append(
                    str(first_char.graphicstate.ncolor)
                    if not isinstance(first_char.graphicstate.ncolor, tuple)
                    else str(first_char.graphicstate.ncolor)
                )
                dict_res["text"].append(e.get_text().replace("\n", " ").rstrip())

    return pd.DataFrame(dict_res)


def pull_fwf(file, **kwargs):
    df = pd.read_fwf(file, **kwargs)
    return df


def pull_xml(file, **kwargs):
    df = pd.read_xml(file, **kwargs)
    return df


def get_source_kwargs(
    read_x: Union[ec.ReadCsv, ec.ReadExcel, ec.ReadFwf, ec.ReadXml],
    frame: ec.Source,
    nrows: Optional[int] = None,
):
    kwargs = {}
    if read_x:
        kwargs = read_x.model_dump(exclude_none=True)

    for k, v in kwargs.items():
        if v == "None":
            kwargs[k] = None

    root_kwargs = (
        "nrows",
        "dtype",
        "sheet_name",
        "names",
        "encoding",
        "low_memory",
        "sep",
    )
    for k in root_kwargs:
        if hasattr(frame, k) and getattr(frame, k):
            if k == "dtype":
                dtypes = getattr(frame, "dtype")
                kwargs["dtype"] = {k: v for k, v in dtypes.items() if v != "date"}
            else:
                kwargs[k] = getattr(frame, k)

    if nrows:
        kwargs["nrows"] = nrows

    return kwargs


def get_target_kwargs(to_x, frame: ec.Target, nrows: Optional[int] = None):
    kwargs = {}
    if to_x:
        kwargs = to_x.model_dump(exclude_none=True)

    root_kwargs = (
        "nrows",
        "dtype",
        "sheet_name",
        "names",
        "encoding",
        "low_memory",
        "sep",
    )
    for k in root_kwargs:
        if hasattr(frame, k) and getattr(frame, k):
            kwargs[k] = getattr(frame, k)
    if nrows:
        kwargs["nrows"] = nrows

    return kwargs


def pull_frame(
    frame: Union[ec.Source, ec.Target],
    nrows: Optional[int] = None,
    sample: bool = False,
) -> pd.DataFrame:
    if frame.type_is_db:
        kwargs = get_source_kwargs(None, frame, nrows)

        sql_io = el.fetch_df_container(sq.SQLDBContainer, frame.url)
        table_io = sql_io[frame.table]
        df = table_io.read(kwargs, sample=sample)
    elif frame.type in (".csv", ".tsv"):
        if isinstance(frame, ec.Source):
            clean_last_column = True
            kwargs = get_source_kwargs(frame.read_csv, frame, nrows)
            if frame.type == ".tsv":
                kwargs["sep"] = "\t"
        else:
            clean_last_column = False
            kwargs = {}
        if "sep" not in kwargs.keys():
            kwargs["sep"] = ","

        kwargs["clean_last_column"] = clean_last_column
        csv_container = el.fetch_df_container(csv.CSVIO, frame.url)
        csv_io = csv_container[frame.table]
        df = csv_io.read(kwargs)

    elif frame.type_is_excel:
        if isinstance(frame, ec.Source):
            kwargs = get_source_kwargs(frame.read_excel, frame, nrows)
        elif isinstance(frame, ec.Target):
            kwargs = get_target_kwargs(frame.to_excel, frame, nrows)
            if "startrow" in kwargs:
                startrow = kwargs.pop("startrow")
                if startrow > 0:
                    kwargs["skiprows"] = startrow + 1
        else:
            kwargs = {}

        xl_io = el.fetch_df_container(xl.ExcelIO, frame.url)
        sheet_io = xl_io[frame.table]
        df = sheet_io.read(kwargs, sample=sample)
    elif frame.type in ("dict"):
        df_dict_io = el.fetch_df_container(pn.DataFrameDictIO, frame.url)
        # df_dict_io = el.fetch_df_dict_io(frame.url)
        df_io = df_dict_io[frame.table]
        df = df_io.read()
    elif frame.type == ".fwf":
        if isinstance(frame, ec.Source):
            kwargs = get_source_kwargs(frame.read_fwf, frame, nrows)
        else:
            kwargs = {}

        df = pull_fwf(frame.url, **kwargs)
    elif frame.type == ".pdf":
        # TODO parallelize, break job into page chunks
        df = None
        for extract_props in el.listify(frame.extract_pages_pdf):
            if isinstance(frame, ec.Source) and extract_props:
                kwargs = extract_props.model_dump(exclude_none=True)
                laparams = None
                if "laparams" in kwargs:
                    laparams = kwargs.pop("laparams")
            else:
                kwargs = {}
            if df is None:
                df = pull_pdf(frame.url, laparams=laparams, **kwargs)
            else:
                df = pd.concat([df, pull_pdf(frame.url, laparams=laparams, **kwargs)])
    elif frame.type == ".xml":
        if isinstance(frame, ec.Source):
            kwargs = get_source_kwargs(frame.read_xml, frame)
        else:
            kwargs = {}
        if "nrows" in kwargs:
            kwargs.pop("nrows")
        df = pull_xml(frame.url, **kwargs)
        if nrows:
            df = df.head(nrows)
    else:
        raise Exception("unable to pull df")

    if frame and hasattr(frame, "dtype") and frame.dtype:
        for k, v in frame.dtype.items():
            if v == "date" and not isinstance(type(df[k]), np.dtypes.DateTime64DType):
                df[k] = pd.to_datetime(df[k])
    return pd.DataFrame(df)


def stack_columns(df, stack: ec.StackDynamic):
    # Define the primary column headers based on the first columns
    primary_headers = list(df.columns[: stack.stack_fixed_columns])

    # Extract the top-level column names from the primary headers
    top_level_headers, _ = zip(*primary_headers)

    # Set the DataFrame's index to the primary headers
    df = df.set_index(primary_headers)

    # Get the names of the newly set indices
    current_index_names = list(df.index.names[: stack.stack_fixed_columns])

    # Create a dictionary to map the current index names to the top-level headers
    index_name_mapping = dict(zip(current_index_names, top_level_headers))

    # Rename the indices using the created mapping
    df.index.rename(index_name_mapping, inplace=True)

    # Stack the DataFrame based on the top-level columns
    df = df.stack(level=stack.stack_header, future_stack=True)

    # Rename the new index created by the stacking operation
    df.index.rename({None: stack.stack_name}, inplace=True)

    # Reset the index for the resulting DataFrame
    df.reset_index(inplace=True)

    return df


def get_configs(config: ec.Config):
    target = config.target
    source = config.source
    transform = config.transform_list

    return target, source, transform


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
    target, source, transform = get_configs(config)
    consistent = config_frames_consistent(config)
    if not target or not target.table or consistent or target.consistency == "ignore":
        # TODO: why is nrows on config root and not in source
        # this is the only place where nrows is passed to pull_frame
        source_df = pull_frame(source, config.nrows, False)
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
    if target.type_is_db:
        sql_container = el.fetch_df_container(sq.SQLDBContainer, target.url)
        return target.table in sql_container
    elif target.type in (".csv", ".tsv"):
        res = target.file_exists
    elif (
        target.type in (".xlsx") and target.file_exists
    ):  # TODO: add other file types supported by Calamine, be careful not to support legacy excel
        # check if sheet exists
        xl_io = el.fetch_df_container(xl.ExcelIO, target.url)
        res = target.sheet_name in xl_io
    elif target.type == "dict":
        # TODO, make these method calls consistent
        # df_dict_io = el.fetch_df_dict_io(target.url)
        df_dict_io = el.fetch_df_container(pn.DataFrameDictIO, target.url)
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
