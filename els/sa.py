import os
from functools import cached_property

import pandas as pd
from python_calamine import CalamineWorkbook, SheetTypeEnum, SheetVisibleEnum

import els.config as ec
import els.core as el
import els.pd as epd


class SQLTable(epd.DataFrameIO):
    def __init__(
        self,
        name,
        mode="r",
        df=pd.DataFrame(),
        read_sql={},
        to_sql={},
        truncate=False,
    ):
        super().__init__(df, name, mode)
        self.read_sql = read_sql
        self.to_sql: ec.ToSql = to_sql
        self.truncate = truncate

        # TODO not efficient to pull table if not being used
        if self.df.empty:
            self.df = self.pull()

    def pull(self, kwargs=None):
        if not kwargs:
            kwargs = self.read_sql
        if self.mode == "r" and self.read_sql != kwargs:
            if "engine" not in kwargs:
                kwargs["engine"] = "calamine"
            if "sheet_name" not in kwargs:
                kwargs["sheet_name"] = self.name
            self.df = pd.read_excel(self.parent.file_io, **kwargs)
            self.read_sql = kwargs
        return self.df

        with sa.create_engine(frame.db_connection_string).connect() as sqeng:
            stmt = sa.select(sa.text("*")).select_from(sa.text(frame.sqn)).limit(nrows)
            df = pd.read_sql(stmt, con=sqeng, **kwargs)

    @property
    def parent(self) -> "SQLEngine":
        return super().parent

    @parent.setter
    def parent(self, v):
        epd.DataFrameIO.parent.fset(self, v)


class SQLEngine(epd.DataFrameContainerMixinIO):
    def __init__(self, url, replace=False):
        self.child_class = SQLTable

        self.url = url
        self.replace = replace

        # load file and sheets
        self.file_io = el.fetch_sa_cn(self.url)
        self.children = [] if self.mode == "w" else self._children_init()

    def _children_init(self):
        self.file_io.seek(0)
        with CalamineWorkbook.from_filelike(self.file_io) as workbook:
            return [
                ExcelSheetIO(
                    startrow=workbook.get_sheet_by_name(sheet.name).total_height + 1,
                    name=sheet.name,
                )
                for sheet in workbook.sheets_metadata
                if (sheet.visible in [SheetVisibleEnum.Visible])
                and (sheet.typ == SheetTypeEnum.WorkSheet)
            ]

    @cached_property
    def create_or_replace(self):
        if self.replace or not os.path.isfile(self.url):
            return True
        else:
            return False

    def get_child(self, child_name) -> ExcelSheetIO:
        return super().get_child(child_name)

    @property
    def childrens(self) -> tuple[ExcelSheetIO]:
        return super().children

    def pull_sheet(self, kwargs):
        sheet_name = kwargs["sheet_name"]
        if self.has_child(sheet_name):
            sheet = self.get_child(sheet_name)
            return sheet.pull(kwargs)
        else:
            raise Exception(f"sheet not found: {sheet_name}")

    @property
    def write_engine(self):
        if self.mode == "a":
            return "openpyxl"
        else:
            return "xlsxwriter"

    def persist(self):
        if self.mode == "w":
            with pd.ExcelWriter(
                self.file_io, engine=self.write_engine, mode=self.mode
            ) as writer:
                for df_io in self.childrens:
                    df = df_io.open_df
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
                    self.file_io,
                    engine=self.write_engine,
                    mode=self.mode,
                    if_sheet_exists=sheet_exist,
                ) as writer:
                    for df_io in self.childrens:
                        if df_io.mode != "r" and df_io.if_sheet_exists == sheet_exist:
                            df = df_io.open_df
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
            self.file_io.seek(0)
            write_file.write(self.file_io.getbuffer())

    def set_sheet_df(self, sheet_name, df, if_exists, kwargs):
        df_io: ExcelSheetIO = self.fetch_df_io(df_name=sheet_name, df=df)
        df_io.set_df(df_name=sheet_name, df=df, if_exists=if_exists)
        if if_exists == "truncate":
            df_io.truncate = True
        df_io.to_excel = kwargs

    def close(self):
        for df_io in self.childrens:
            df_io.close()
        self.file_io.close()
        del el.open_files[self.url]
