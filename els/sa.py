import logging
import os
from typing import Literal, Optional
from urllib.parse import parse_qs, urlencode, urlparse

import pandas as pd
import sqlalchemy as sa

import els.config as ec
import els.core as el
import els.pd as epd


def lcase_dict_keys(_dict):
    return {k.lower(): v for k, v in _dict.items()}


def lcase_query_keys(query):
    query_parsed = parse_qs(query)
    return lcase_dict_keys(query_parsed)


class SQLTable(epd.DataFrameIO):
    def __init__(
        self,
        name,
        parent,
        if_exists="fail",
        mode="s",
        df=pd.DataFrame(),
        kw_for_pull={},
        kw_for_push={},
    ):
        super().__init__(
            df=df,
            name=name,
            parent=parent,
            mode=mode,
            if_exists=if_exists,
        )
        self.kw_for_pull = kw_for_pull
        self.kw_for_push: ec.ToSql = kw_for_push

    @property
    def sqn(self) -> str:
        if self.parent.flavor == "duckdb":
            res = '"' + self.name + '"'
        # elif self.dbschema and self.table:
        #     res = "[" + self.dbschema + "].[" + self.table + "]"
        else:
            res = "[" + self.name + "]"
        return res

    @property
    def truncate_stmt(self):
        if self.parent.flavor == "sqlite":
            return f"delete from {self.sqn}"
        else:
            return f"truncate table {self.sqn}"

    def _read(self, kwargs, sample: bool = False):
        print(f"READ kwargs:{kwargs}")
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
                sa.select(sa.text("*")).select_from(sa.text(f"{self.sqn}")).limit(nrows)
            )
            self.df = pd.read_sql(stmt, con=sqeng, **kwargs)
            if sample:
                self.df_target = epd.get_column_frame(self.df)
            else:
                self.df_target = self.df
            print(f"READ result: {self.df}")

    @property
    def parent(self) -> "SQLDBContainer":
        return super().parent

    @parent.setter
    def parent(self, v):
        epd.DataFrameIO.parent.fset(self, v)


class SQLDBContainer(epd.DataFrameContainerMixinIO):
    def __init__(self, url, replace=False):
        self.child_class = SQLTable

        self.url = url
        self.replace = replace

        self.sa_engine: sa.Engine = el.fetch_sa_engine(
            self.db_connection_string,
            replace=replace,
        )
        self._children_init()
        print(f"children created: {[n.name for n in self.children]}")

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
        if explicit_driver and explicit_driver in el.supported_mssql_odbc_drivers:
            return explicit_driver
        else:
            return None

    @property
    def odbc_driver_supported_available(self):
        explicit_odbc = self.db_url_driver
        if explicit_odbc and explicit_odbc in el.supported_available_odbc_drivers():
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
                query = el.lcase_query_keys(url_parsed.query)
                query["driver"] = query["driver"][0]
                if query["driver"].lower() == "odbc driver 18 for sql server":
                    query["TrustServerCertificate"] = "yes"
                res = url_parsed._replace(query=urlencode(query)).geturl()
                # res = url_parsed.geturl()
            elif len(el.supported_available_odbc_drivers()):
                logging.info(
                    "No valid ODBC driver defined in connection string, choosing one."
                )
                query = lcase_query_keys(url_parsed.query)
                query["driver"] = list(el.supported_available_odbc_drivers())[0]
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
    def flavor(
        self,
    ) -> Literal[
        "mssql",
        "duckdb",
        "sqlite",
    ]:
        scheme = self.url.split(":")[0]
        return scheme.split("+")[0]

    @property
    def dbtype(
        self,
    ) -> Literal[
        "file",
        "server",
    ]:
        if self.flavor in ("sqlite", "duckdb"):
            return "file"
        else:
            return "server"

    def db_exists(self) -> bool:
        return True

    def _children_init(self):
        with self.sa_engine.connect() as sqeng:
            inspector = sa.inspect(sqeng)
            # inspector.get_table_names(source.dbschema)
            [
                SQLTable(
                    name=n,
                    parent=self,
                )
                for n in inspector.get_table_names()
            ]

    @property
    def create_or_replace(self):
        # if self.replace or not os.path.isfile(self.url):
        # TODO: add logic which discriminates between file or server-based databases
        # consider allowing database replacement with prompt
        if (
            self.replace
            or (self.dbtype == "file" and not os.path.isfile(self.url))
            or (self.dbtype == "server" and not self.db_exists())
        ):
            return True
        else:
            return False

    def get_child(self, child_name) -> SQLTable:
        return super().get_child(child_name)

    @property
    def childrens(self) -> tuple[SQLTable]:
        return super().children

    def db_create(self):
        if self.dbtype == "file":
            pass  # TODO: why does this not seem to be necessary??
        elif self.dbtype == "server":
            pass  # maybe this is done better at the fetch level??

    def persist(self):
        # print(f"children len: {len(self.children)}")
        if self.mode == "w":
            self.db_create()
        with self.sa_engine.connect() as sqeng:
            for df_io in self.childrens:
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
        print("engine disposed")
        del el.open_sa_engs[self.db_connection_string]
