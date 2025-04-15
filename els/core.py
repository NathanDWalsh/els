import io
import os

import pandas as pd

import els.io.csv as csv
import els.io.pd as pn
import els.io.sql as sq
import els.io.xl as xl

default_target: dict[str, pd.DataFrame] = {}
url_dicts: dict[str, dict[str, pd.DataFrame]] = {}

io_files: dict[str, io.BytesIO] = {}
io_workbooks: dict[str, xl.ExcelIO] = {}
io_csvs: dict[str, csv.CSVIO] = {}
io_dicts: dict[int, pn.DataFrameDictIO] = {}
io_sqls: dict[str, sq.SQLDBContainer] = {}


def fetch_sql_container(url: str, replace: bool = False) -> sq.SQLDBContainer:
    if url is None:
        raise Exception("Cannot fetch None url")
    elif url in io_sqls:
        res = io_sqls[url]
    else:
        res = sq.SQLDBContainer(url, replace)
    io_sqls[url] = res
    return res


def fetch_df_dict(
    url: str,
    replace: bool = False,
):
    res = url_dicts[url]
    if replace:
        res.clear()
    return res


def urlize_dict(df_dict: dict[str, pd.DataFrame]):
    res = f"dict://{id(df_dict)}"
    if res not in url_dicts:
        url_dicts[res] = df_dict
    return res


def fetch_df_dict_io(
    url: str,
    replace: bool = False,
):
    if isinstance(url, str):
        if url in io_dicts:
            res = io_dicts[url]
        else:
            res = pn.DataFrameDictIO(url, replace)
    else:
        raise Exception(f"Cannot fetch df_dict_io from type: {type(url)}")
    io_dicts[url] = res
    return res


def fetch_file_io(url: str, replace: bool = False):
    if url is None:
        raise Exception("Cannot fetch None url")
    elif url in io_files:
        res = io_files[url]
    # only allows replacing once:
    elif replace:
        res = io.BytesIO()
    # chck file exists:
    elif os.path.isfile(url):
        with open(url, "rb") as file:
            res = io.BytesIO(file.read())
    else:
        res = io.BytesIO()
    io_files[url] = res
    return res


def fetch_csv_io(url: str, replace: bool = False):
    if url is None:
        raise Exception("Cannot fetch None url")
    elif url in io_csvs:
        res = io_csvs[url]
    else:
        res = csv.CSVIO(url, replace)
    io_csvs[url] = res
    return res


def fetch_excel_io(url: str, replace: bool = False):
    if url is None:
        raise Exception("Cannot fetch None url")
    elif url in io_workbooks:
        res = io_workbooks[url]
    else:
        res = xl.ExcelIO(url, replace)
    io_workbooks[url] = res
    return res


def listify(v):
    return v if isinstance(v, (list, tuple)) else [v]
