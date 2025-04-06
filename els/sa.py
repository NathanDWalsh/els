import os
from typing import Literal

import pandas as pd
import sqlalchemy as sa

import els.config as ec
import els.core as el
import els.pd as epd


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

    # def pull(self, kwargs={}, nrows=100):
    def _read(self, kwargs):
        print(f"READ kwargs:{kwargs}")
        if not kwargs:
            kwargs = self.kw_for_pull
        else:
            self.kw_for_pull = kwargs
        if "nrows" in kwargs:
            nrows = kwargs.pop("nrows")
        else:
            nrows = None
        sample = False
        if nrows == 100:
            sample = True
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
            self.url,
            replace=replace,
        )
        self._children_init()
        print(f"children created: {[n.name for n in self.children]}")

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
        del el.open_sa_engs[self.url]
