import io
import os

import pandas as pd

import els.pd as pn
import els.xl as xl

default_target = {}
open_files = {}
open_workbooks = {}
open_dicts = {}
open_dfs = {}


def fetch_df_old(df):
    if df is None:
        raise Exception("Cannot fetch None df")
    elif isinstance(df, int):
        return df, open_dfs[df]
    else:
        df_id = id(df)
        open_dfs[df_id] = df
        return df_id, df


def fetch_df(df_id):
    return open_dfs[df_id]


def fetch_df_id(df):
    if df is None:
        raise Exception("Cannot fetch None df")
    else:
        df_id = id(df)
        open_dfs[df_id] = df
        return df_id


def set_df(df_id, df):
    open_dfs[df_id] = df


def fetch_df_dict_io(df_dict: dict, replace: bool = False):
    if isinstance(df_dict, int):
        return open_dicts[df_dict]

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


def get_column_frame(df):
    column_frame = pd.DataFrame(columns=df.columns, index=None, data=None)
    column_frame = column_frame.astype(df.dtypes)
    return column_frame


def append_into(dfs):
    # appends subsequent dfs into the first df, keeping only the columns from the first
    ncols = len(dfs[0].columns)
    return pd.concat(dfs, ignore_index=True).iloc[:, 0:ncols]
