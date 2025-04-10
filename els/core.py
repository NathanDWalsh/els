import io
import os

import pandas as pd

import els.pd as pn
import els.sa as sq
import els.xl as xl

default_target: dict[str, pd.DataFrame] = {}

open_files: dict[str, io.BytesIO] = {}
open_workbooks: dict[str, xl.ExcelIO] = {}

open_dicts: dict[int, pn.DataFrameDictIO] = {}

open_sqls: dict[str, sq.SQLDBContainer] = {}


def fetch_sql_container(url: str, replace: bool = False) -> sq.SQLDBContainer:
    if url is None:
        raise Exception("Cannot fetch None url")
    elif url in open_sqls:
        res = open_sqls[url]
    else:
        res = sq.SQLDBContainer(url, replace)
    open_sqls[url] = res
    return res


def urlize_dict(df_dict: dict):
    fetch_df_dict_io(df_dict)
    return f"dict://{id(df_dict)}"


def fetch_df_dict_io(df_dict: dict, replace: bool = False):
    if isinstance(df_dict, int):
        return open_dicts[df_dict]
    if isinstance(df_dict, str):
        return open_dicts[int(df_dict.split("/")[-1])]
    if df_dict is None:
        raise Exception("Cannot fetch None dict")
    elif id(df_dict) in open_dicts:
        res = open_dicts[id(df_dict)]
    else:
        res = pn.DataFrameDictIO(df_dict, replace)
    open_dicts[id(df_dict)] = res
    return res


def fetch_file_io(url: str, replace: bool = False):
    if url is None:
        raise Exception("Cannot fetch None url")
    elif url in open_files:
        res = open_files[url]
    # only allows replacing once:
    elif replace:
        res = io.BytesIO()
    # chck file exists:
    elif os.path.isfile(url):
        with open(url, "rb") as file:
            res = io.BytesIO(file.read())
    else:
        res = io.BytesIO()
    open_files[url] = res
    return res


def fetch_excel_io(url: str, replace: bool = False):
    if url is None:
        raise Exception("Cannot fetch None url")
    elif url in open_workbooks:
        res = open_workbooks[url]
    else:
        res = xl.ExcelIO(url, replace)
    open_workbooks[url] = res
    return res


def listify(v):
    return v if isinstance(v, (list, tuple)) else [v]
