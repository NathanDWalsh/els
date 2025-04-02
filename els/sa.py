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
        mode="r",
        df=pd.DataFrame(),
        read_sql={},
        to_sql={},
        truncate=False,
    ):
        super().__init__(df, name, parent, mode)
        self.read_sql = read_sql
        self.to_sql: ec.ToSql = to_sql
        self.truncate = truncate

        # TODO not efficient to pull table if not being used
        if self.df.empty:
            self.df = self.pull()

    def pull(self, kwargs={}, nrows=100):
        if not kwargs:
            kwargs = self.read_sql
        else:
            self.read_sql = kwargs
        if "nrows" in kwargs:
            kwargs.pop("nrows")
        if not self.parent.url:
            raise Exception("invalid db_connection_string")
        if not self.name:
            raise Exception("invalid sqn")
        with sa.create_engine(self.parent.url).connect() as sqeng:
            stmt = sa.select(sa.text("*")).select_from(sa.text(self.name)).limit(nrows)
            df = pd.read_sql(stmt, con=sqeng, **kwargs)
        return df

        # if self.mode == "r" and self.read_sql != kwargs:
        #     if "engine" not in kwargs:
        #         kwargs["engine"] = "calamine"
        #     if "sheet_name" not in kwargs:
        #         kwargs["sheet_name"] = self.name
        #     self.df = pd.read_excel(self.parent.sa_engine, **kwargs)
        #     self.read_sql = kwargs
        # return self.df

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

        # load file and sheets
        self.sa_engine: sa.Engine = el.fetch_sa_engine(self.url)
        self.children = [] if self.mode == "w" else self._children_init()
        print(f"children created: {[n.name for n in self.children]}")

    def _children_init(self):
        with self.sa_engine.connect() as sqeng:
            inspector = sa.inspect(sqeng)
            # inspector.get_table_names(source.dbschema)
            return [
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

    def pull_table(self, kwargs, nrows, sqn):
        if self.has_child(sqn):
            table = self.get_child(sqn)
            return table.pull(kwargs, nrows)
        else:
            raise Exception(
                f"table {sqn} not found in {[n.name for n in self.children]}"
            )

    def persist(self):
        if self.mode == "w":
            with pd.ExcelWriter(
                self.sa_engine, engine=self.write_engine, mode=self.mode
            ) as writer:
                for df_io in self.childrens:
                    df = df_io.df_target
                    to_excel = df_io.to_excel
                    if to_excel:
                        kwargs = to_excel.model_dump(exclude_none=True)
                    else:
                        kwargs = {}
                    # TODO integrate better into write method?
                    if isinstance(df.columns, pd.MultiIndex):
                        df = epd.multiindex_to_singleindex(df)
                    df.to_excel(writer, index=False, sheet_name=df_io.name, **kwargs)
                for sheet in writer.sheets.values():
                    sheet.autofit(500)
        elif self.mode == "a":
            sheet_exists = set()
            for df_io in self.childrens:
                if df_io.mode != "r":
                    sheet_exists.add(df_io.if_sheet_exists)
            for sheet_exist in sheet_exists:
                with pd.ExcelWriter(
                    self.sa_engine,
                    engine=self.write_engine,
                    mode=self.mode,
                    if_sheet_exists=sheet_exist,
                ) as writer:
                    for df_io in self.childrens:
                        if df_io.mode != "r" and df_io.if_sheet_exists == sheet_exist:
                            df = df_io.df_target
                            to_excel = df_io.to_excel
                            if df_io.mode == "a":
                                header = False
                            else:
                                header = True
                            if to_excel:
                                kwargs = to_excel.model_dump(exclude_none=True)
                            else:
                                kwargs = {}
                            # TODO integrate better into write method?
                            if isinstance(df.columns, pd.MultiIndex):
                                df = epd.multiindex_to_singleindex(df)
                            df.to_excel(
                                writer,
                                index=False,
                                sheet_name=df_io.name,
                                header=header,
                                startrow=df_io.startrow,
                                **kwargs,
                            )

        with open(self.url, "wb") as write_file:
            self.sa_engine.seek(0)
            write_file.write(self.sa_engine.getbuffer())

    def set_table_df(self, table_name, df, if_exists, kwargs):
        df_io: SQLTable = self.fetch_child(df_name=table_name, df=df)
        df_io.set_df(df_name=table_name, df=df, if_exists=if_exists)
        if if_exists == "truncate":
            df_io.truncate = True
        df_io.to_sql = kwargs

    def close(self):
        for df_io in self.childrens:
            df_io.close()
        self.sa_engine.dispose()
        print("engine disposed")
        del el.open_sa_engs[self.url]
