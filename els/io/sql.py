from __future__ import annotations

import logging
from typing import Any, Generator, Literal, Optional, Union
from urllib.parse import parse_qs, urlencode, urlparse

import pandas as pd
import pyodbc  # type: ignore
import sqlalchemy as sa
from sqlalchemy_utils import create_database, drop_database  # type: ignore
from sqlalchemy_utils.functions.orm import quote  # type: ignore

import els.config as ec
from els.sa_utils_fork import database_exists

from .base import ContainerWriterABC, FrameABC


def lcase_dict_keys(_dict: dict[str, Any]):
    return {k.lower(): v for k, v in _dict.items()}


def lcase_query_keys(query):
    query_parsed = parse_qs(query)
    return lcase_dict_keys(query_parsed)


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


def fetch_sa_engine(url, replace: bool = False) -> sa.Engine:
    dialect = sa.make_url(url).get_dialect().name
    driver = sa.make_url(url).get_driver_name()
    kwargs = {}
    if (
        dialect in ("mssql")
        and driver == "pyodbc"
        and len(supported_available_odbc_drivers())
    ):
        kwargs["fast_executemany"] = True

    if url is None:
        raise Exception("Cannot fetch None url")
    else:
        if not database_exists(url):
            create_database(url)
        elif replace:
            drop_database(url)
            create_database(url)
        res = sa.create_engine(url, **kwargs)
    return res


class SQLFrame(FrameABC):
    def __init__(
        self,
        name,
        parent,
        if_exists="fail",
        mode="s",
        df=pd.DataFrame(),
        kw_for_pull=None,  # TODO: fix mutable default
        kw_for_push={},
    ) -> None:
        super().__init__(
            df=df,
            name=name,
            parent=parent,
            mode=mode,
            if_exists=if_exists,
            kw_for_pull=kw_for_pull,
        )
        self.kw_for_push: ec.ToSQL = kw_for_push

    @property
    def sqn(self) -> str:
        if self.parent.dialect_name == "duckdb":
            res = '"' + self.name + '"'
        # elif self.dbschema and self.table:
        #     res = "[" + self.dbschema + "].[" + self.table + "]"
        else:
            res = "[" + self.name + "]"
        return res

    @property
    def truncate_stmt(self):
        if self.parent.dialect_name == "sqlite":
            return f"delete from {self.sqn}"
        else:
            return f"truncate table {self.sqn}"

    def _read(self, kwargs):
        if not kwargs:
            kwargs = self.kw_for_pull
        else:
            self.kw_for_pull = kwargs
        if "nrows" in kwargs:
            nrows = kwargs.pop("nrows")
        else:
            nrows = None
        if not self.parent.url:
            raise Exception("invalid db_connection_string")
        if not self.name:
            raise Exception("invalid sqn")
        with self.parent.sa_engine.connect() as sqeng:
            stmt = (
                sa.select(sa.text("*"))
                .select_from(sa.text(f"{quote(sqeng, self.name)}"))
                .limit(nrows)
            )
            self.df = pd.read_sql(stmt, con=sqeng, **kwargs)

    @property
    def parent(self) -> SQLContainer:
        return super().parent

    @parent.setter
    def parent(self, v):
        FrameABC.parent.fset(self, v)


class SQLContainer(ContainerWriterABC):
    def __init__(self, url, replace=False):
        super().__init__(SQLFrame, url, replace)

    def __iter__(self) -> Generator[SQLFrame, None, None]:
        for child in super().children:
            yield child

    @property
    def query_lcased(self):
        url_parsed = urlparse(self.url)
        query = parse_qs(url_parsed.query)
        res = {k.lower(): v[0].lower() for k, v in query.items()}
        return res

    @property
    def db_url_driver(self):
        query_lcased = self.query_lcased
        if "driver" in query_lcased.keys():
            return query_lcased["driver"]
        else:
            return False

    @property
    def choose_db_driver(self):
        explicit_driver = self.db_url_driver
        if explicit_driver and explicit_driver in supported_mssql_odbc_drivers:
            return explicit_driver
        else:
            return None

    @property
    def odbc_driver_supported_available(self):
        explicit_odbc = self.db_url_driver
        if explicit_odbc and explicit_odbc in supported_available_odbc_drivers():
            return True
        else:
            return False

    @property
    def type(self):
        return self.url.split(":")[0]

    @property
    def db_connection_string(self) -> Optional[str]:
        # Define the connection string based on the database type
        if self.type in (
            "mssql+pymssql",
            "mssql+pyodbc",
        ):  # assumes advanced usage and url must be correct
            return self.url
        elif (
            self.type == "mssql"
        ):  # try to automatically detect odbc drivers and falls back on tds/pymssql
            url_parsed = urlparse(self.url)._replace(scheme="mssql+pyodbc")
            if self.odbc_driver_supported_available:
                # TODO: fix el.
                query = lcase_query_keys(url_parsed.query)
                query["driver"] = query["driver"][0]
                if query["driver"].lower() == "odbc driver 18 for sql server":
                    query["TrustServerCertificate"] = "yes"
                res = url_parsed._replace(query=urlencode(query)).geturl()
                # res = url_parsed.geturl()
            elif len(supported_available_odbc_drivers()):
                logging.info(
                    "No valid ODBC driver defined in connection string, choosing one."
                )
                query = lcase_query_keys(url_parsed.query)
                query["driver"] = list(supported_available_odbc_drivers())[0]
                logging.info(query["driver"].lower())
                if query["driver"].lower() == "odbc driver 18 for sql server":
                    query["TrustServerCertificate"] = "yes"
                res = url_parsed._replace(query=urlencode(query)).geturl()
            else:
                logging.info("No ODBC drivers for pyodbc, using pymssql")
                res = urlparse(self.url)._replace(scheme="mssql+pymssql").geturl()
        elif self.type in ("sqlite", "duckdb"):
            res = self.url
        elif self.type == "postgres":
            res = "Driver={{PostgreSQL}};Server={self.server};Database={self.database};"
        else:
            res = None
        return res

    @property
    def dialect_name(
        self,
    ) -> Union[
        Literal[
            "mssql",
            "duckdb",
            "sqlite",
        ],
        str,
    ]:
        assert self.db_connection_string
        url = sa.make_url(self.db_connection_string)
        dialect = url.get_dialect()
        return dialect.name

    @property
    def dbtype(
        self,
    ) -> Literal[
        "file",
        "server",
    ]:
        if self.dialect_name in ("sqlite", "duckdb"):
            return "file"
        else:
            return "server"

    def _children_init(self) -> None:
        self.sa_engine: sa.Engine = fetch_sa_engine(self.db_connection_string)
        with self.sa_engine.connect() as sqeng:
            inspector = sa.inspect(sqeng)
            [
                SQLFrame(
                    name=n,
                    parent=self,
                )
                for n in inspector.get_table_names()
            ]

    @property
    def create_or_replace(self):
        if self.replace or not database_exists(self.db_connection_string):
            return True
        else:
            return False

    def persist(self):
        if self.mode == "w":
            self.sa_engine = fetch_sa_engine(
                self.db_connection_string,
                replace=True,
            )
        with self.sa_engine.connect() as sqeng:
            for df_io in self:
                if df_io.mode in ("a", "w"):
                    if df_io.kw_for_push:
                        kwargs = df_io.kw_for_push
                    else:  # TODO: else maybe not needed when default for kw_for_push
                        kwargs = {}
                    if df_io.if_exists == "truncate":
                        sqeng.execute(sa.text(df_io.truncate_stmt))
                        df_io.if_exists = "append"
                    df_io.df_target.to_sql(
                        df_io.name,
                        sqeng,
                        schema=None,
                        index=False,
                        if_exists=df_io.if_exists,
                        chunksize=1000,
                        **kwargs,
                    )
            sqeng.connection.commit()

    def close(self):
        self.sa_engine.dispose()
