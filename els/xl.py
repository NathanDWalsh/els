import os
from functools import cached_property

import pandas as pd
from anytree import NodeMixin
from python_calamine import CalamineWorkbook, SheetTypeEnum, SheetVisibleEnum

import els.core as el
import els.pd as epd


def get_sheet_names(
    xl_io, sheet_states: list = [SheetVisibleEnum.Visible]
) -> list[str]:
    xl_io.seek(0)
    with CalamineWorkbook.from_filelike(xl_io) as workbook:
        worksheet_names = [
            sheet.name
            for sheet in workbook.sheets_metadata
            if (sheet.visible in sheet_states)
            and (sheet.typ == SheetTypeEnum.WorkSheet)
        ]
        return worksheet_names


def get_sheet_height(xl_io, sheet_name: str) -> int:
    xl_io.seek(0)
    with CalamineWorkbook.from_filelike(xl_io) as workbook:
        if sheet_name in workbook.sheet_names:
            return workbook.get_sheet_by_name(sheet_name).total_height
        else:
            return None


def get_sheet_row(xl_io, sheet_name: str, row_index: int) -> list:
    xl_io.seek(0)
    with CalamineWorkbook.from_filelike(xl_io) as workbook:
        if sheet_name in workbook.sheet_names:
            return workbook.get_sheet_by_name(sheet_name).to_python(
                nrows=row_index + 1
            )[-1]
        else:
            return None


class ExcelSheetIO(epd.DataFrameIO):
    def __init__(
        self,
        parent,
        name,
        mode="r",
        df=pd.DataFrame(),
        startrow=0,
        read_excel={},
        to_excel={},
        truncate=False,
    ):
        super().__init__(df, parent, name, mode)
        self._startrow = startrow
        self.read_excel = read_excel
        self.to_excel = to_excel
        self.truncate = truncate

    @property
    def if_sheet_exists(self):
        if self.mode == "a":
            return "overlay"
        elif self.mode == "w":
            return "replace"
        else:
            return None

    @property
    def startrow(self):
        if self.truncate or self.mode =='w':
            return 0
        else:
            return self._startrow
        

    @startrow.setter
    def startrow(self,v):
        self._startrow = v

    @property
    def column_frame(self):
        if self.df.empty:
            self.df = self.parent.pull_sheet({'sheet_name':self.name})
        res = el.get_column_frame(self.df)
        return res
        

    def pull(self, kwargs):
        if self.mode == "r" and self.read_excel != kwargs:
            if "engine" not in kwargs:
                kwargs["engine"] = "calamine"
            if "sheet_name" not in kwargs:
                kwargs["sheet_name"] = self.name
            self.df = pd.read_excel(self.parent.file_io, **kwargs)
            self.read_excel = kwargs
        return self.df

    def close(self):
        if self.df_id in el.open_dfs:
            del el.open_dfs[self.df_id]

    def write(self):
        if self.mode == "a" and not self.open_df.empty:
            # print('write appending into')
            el.open_dfs[self.df_id] = el.append_into([self.open_df, self.df])
        # elif self.mode == "w":
        else:
            # print('write just write')
            el.open_dfs[self.df_id] = self.df


class ExcelIO(NodeMixin):
    def __init__(self, url, replace=False):
        self.url = url
        self.replace = replace

        # load file and sheets
        self.file_io = el.fetch_file_io(self.url, replace=self.replace)
        self.sheet_ios = self.get_sheet_ios()

    def get_sheet_ios(self) -> dict:
        if self.create_or_replace:
            return []
        else:
            self.file_io.seek(0)
            with CalamineWorkbook.from_filelike(self.file_io) as workbook:
                res = [
                    ExcelSheetIO(
                        startrow=workbook.get_sheet_by_name(sheet.name).total_height
                        + 1,
                        name=sheet.name,
                        parent=self,
                    )
                    for sheet in workbook.sheets_metadata
                    if (sheet.visible in [SheetVisibleEnum.Visible])
                    and (sheet.typ == SheetTypeEnum.WorkSheet)
                ]
                return res

    @cached_property
    def create_or_replace(self):
        if self.replace or not os.path.isfile(self.url):
            return True
        else:
            return False

    def get_child(self, child_name):
        for c in self.children:
            if c.name == child_name:
                return c
        return None

    def has_child(self, child_name):
        for c in self.children:
            if c.name == child_name:
                return True
        return False
    
    def add_child(self, child):
        child.parent = self
    

    def pull_sheet(self, kwargs):
        sheet_name = kwargs["sheet_name"]
        if self.has_child(sheet_name):
            sheet = self.get_child(sheet_name)
            if sheet.mode == "r" and sheet.read_excel != kwargs:
                if "engine" not in kwargs:
                    kwargs["engine"] = "calamine"
                sheet.df = pd.read_excel(self.file_io, **kwargs)
                sheet.read_excel = kwargs
            return sheet.df
        else:
            raise Exception(f"sheet not found: {sheet_name}")

    @property
    def write_engine(self):
        if self.mode == "a":
            return "openpyxl"
        else:
            return "xlsxwriter"

    @property
    def any_empty_frames(self):
        for df_io in self.children:
            if df_io.mode not in "r":
                if df_io.df.empty:
                    print(f"cannot write empty dataframe; {df_io.name}: {df_io.df}")
                    return True
        return False

    def write(self):
        if self.mode != "r":
            if self.any_empty_frames:
                raise Exception("Cannot write empty dataframe")
            for df_io in self.children:
                df_io.write()
                # TODO this is pd persist
                # self.df_dict[df_io.name] = df_io.open_df
            self.persist()

    def persist(self):

        if self.mode == "w":
            with pd.ExcelWriter(
                self.file_io, engine=self.write_engine, mode=self.mode
            ) as writer:
                for df_io in self.children:
                    df = df_io.open_df
                    to_excel = df_io.to_excel
                    if to_excel:
                        kwargs = to_excel.model_dump(exclude_none=True)
                    else:
                        kwargs = {}
                    df.to_excel(writer, index=False, sheet_name=df_io.name, **kwargs)
                for sheet in writer.sheets.values():
                    sheet.autofit(500)
        elif self.mode == "a":
            sheet_exists = set()
            for df_io in self.children:
                if df_io.mode != "r":
                    sheet_exists.add(df_io.if_sheet_exists)
            for sheet_exist in sheet_exists:
                with pd.ExcelWriter(
                    self.file_io,
                    engine=self.write_engine,
                    mode=self.mode,
                    if_sheet_exists=sheet_exist,
                ) as writer:
                    for df_io in self.children:
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

    def fetch_df_io(self, df_name, df) -> ExcelSheetIO:
        if not self.has_child(df_name):
            self.add_child(ExcelSheetIO(df=df, parent=self, name=df_name, mode="w"))
        return self.get_child(df_name)

    def set_sheet_df(self, sheet_name, df, if_exists, kwargs):
        df_io = self.fetch_df_io(df_name=sheet_name, df=df)
        df_io.set_df(df_name=sheet_name, df=df, if_exists=if_exists)
        if if_exists == "truncate":
            df_io.truncate = True
        df_io.to_excel = kwargs

    def close(self):
        for df_io in self.children:
            df_io.close()
        self.file_io.close()
        del el.open_files[self.url]

    @property
    def sheet_names(self):
        res = []
        for ws_io in self.children:
            res.append(ws_io.name)
        return res

    @cached_property
    def mode(self):
        if len(self.children) == 0:
            return "r"
        elif self.create_or_replace:
            return "w"
        else:
            for c in self.children:
                if c.mode in ("a", "w"):
                    return "a"
        return "r"
