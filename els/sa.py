from functools import cached_property

import pandas as pd
import sqlalchemy as sa

import els.config as ec
import els.core as el
import els.pd as epd


def get_table_names(source: ec.Source) -> list[str]:
    res = None
    if source.type_is_db:
        if not source.table:
            with sa.create_engine(source.db_connection_string).connect() as sqeng:
                inspector = sa.inspect(sqeng)
                res = inspector.get_table_names(source.dbschema)
        else:
            res = [source.table]
    return res


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

        # # TODO not efficient to pull table if not being used
        # if self.df.empty:
        #     self.df = self.pull()

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
                sa.select(sa.text("*"))
                .select_from(sa.text(f"[{self.name}]"))
                .limit(nrows)
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

        self.sa_engine: sa.Engine = el.fetch_sa_engine(self.url)
        self._children_init()
        print(f"children created: {[n.name for n in self.children]}")

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

    @cached_property
    def create_or_replace(self):
        # if self.replace or not os.path.isfile(self.url):
        # TODO: add logic which discriminates between file or server-based databases
        # consider allowing database replacement with prompt
        if self.replace:
            return True
        else:
            return False

    def get_child(self, child_name) -> SQLTable:
        return super().get_child(child_name)

    @property
    def childrens(self) -> tuple[SQLTable]:
        return super().children

    def persist(self):
        # print(f"children len: {len(self.children)}")

        with self.sa_engine.connect() as sqeng:
            for df_io in self.childrens:
                # print(f"PERSIST: {[df_io.mode, df_io.name]}")
                if df_io.mode in ("a", "w"):
                    # self.df_dict[df_io.name] = df_io.df_target

                    if df_io.kw_for_push:
                        kwargs = df_io.kw_for_push
                    else:  # TODO: else maybe not needed when default for kw_for_push
                        kwargs = {}
                    print(f"TO_SQL::::::: {df_io.df_target}")
                    if df_io.if_exists == "truncate":
                        # TODO: bring back sqn's and flavor specific truncate scenarios
                        sqeng.execute(sa.text(f"delete from [{df_io.name}]"))
                        df_io.if_exists = "append"
                    df_io.df_target.to_sql(
                        df_io.name,
                        sqeng,
                        # schema=target.dbschema,
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
