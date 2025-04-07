import io
import os
from typing import Literal

import pandas as pd
import pyodbc
import sqlalchemy as sa
from sqlalchemy_utils import create_database, database_exists, drop_database

import els.pd as pn
import els.sa as sq
import els.xl as xl

default_target: dict[str, pd.DataFrame] = {}
open_files: dict[str, io.BytesIO] = {}
open_workbooks: dict[str, xl.ExcelIO] = {}
open_dicts: dict[int, pn.DataFrameDictIO] = {}
open_sa_engs: dict[str, sa.Engine] = {}
open_sqls: dict[str, sq.SQLDBContainer] = {}


supported_mssql_odbc_drivers = {
    "sql server native client 11.0",
    "odbc driver 17 for sql server",
    "odbc driver 18 for sql server",
}


def available_odbc_drivers():
    available = pyodbc.drivers()
    lcased = {v.lower() for v in available}
    return lcased


def supported_available_odbc_drivers():
    supported = supported_mssql_odbc_drivers
    available = available_odbc_drivers()
    return supported.intersection(available)


def fetch_sql_container(url: str, replace: bool = False) -> sq.SQLDBContainer:
    if url is None:
        raise Exception("Cannot fetch None url")
    elif url in open_sqls:
        res = open_sqls[url]
    else:
        res = sq.SQLDBContainer(url, replace)
    open_sqls[url] = res
    return res


def fetch_sa_engine(url, replace: bool = False) -> sa.Engine:
    dialect: Literal["mssql", "sqlite", "duckdb"] = url.split(":")[0].split("+")[0]
    kwargs = {}
    if dialect in ("mssql") and len(supported_available_odbc_drivers()):
        kwargs["fast_executemany"] = True

    if url is None:
        raise Exception("Cannot fetch None url")
    elif url in open_sa_engs:
        res = open_sa_engs[url]
    else:
        res = sa.create_engine(url, **kwargs)
        if not database_exists(res.url):
            create_database(res.url)
        elif replace:
            drop_database(res.url)
            create_database(res.url)

    open_sa_engs[url] = res
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
