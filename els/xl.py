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
        self.startrow = startrow
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

    # @property
    # def start

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


class ExcelIO(NodeMixin):
    def __init__(self, url, replace=False):
        self.url = url
        self.replace = replace

        # load file and sheets
        self.file_io = el.fetch_file_io(self.url, replace=self.replace)
        self.sheets = self.get_sheet_deets()
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

    def get_sheet_deets(self) -> dict:
        if self.create_or_replace:
            return {}
        else:
            self.file_io.seek(0)
            with CalamineWorkbook.from_filelike(self.file_io) as workbook:
                res = {
                    sheet.name: {
                        "startrow": workbook.get_sheet_by_name(sheet.name).total_height
                        + 1,
                        "mode": "r",
                        "read_excel": {},
                        "to_excel": {},
                    }
                    for sheet in workbook.sheets_metadata
                    if (sheet.visible in [SheetVisibleEnum.Visible])
                    and (sheet.typ == SheetTypeEnum.WorkSheet)
                }
                return res

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

    def pull_sheet(self, kwargs):
        sheet_name = kwargs["sheet_name"]
        if sheet_name in self.sheets:
            sheet = self.sheets[sheet_name]
            if sheet["mode"] == "r" and sheet["read_excel"] != kwargs:
                if "engine" not in kwargs:
                    kwargs["engine"] = "calamine"
                sheet["df"] = pd.read_excel(self.file_io, **kwargs)
                sheet["read_excel"] = kwargs
            return sheet["df"]
        else:
            raise Exception(f"sheet not found: {sheet_name}")

    def pull_sheet2(self, kwargs):
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

    def pull_sheet_structure(self, sheet_name, sample_rows=100):
        kwargs = {}
        kwargs["sheet_name"] = sheet_name
        kwargs["nrows"] = sample_rows
        df = self.pull_sheet2(kwargs)
        return el.get_column_frame(df)

    def pull_bottom_blanks(self, sheet_name, blankrows):
        kwargs = {}
        kwargs["sheet_name"] = sheet_name
        data_lastrow = self.sheets[sheet_name]["startrow"] - 1
        df = self.sheets[sheet_name]["df"]
        empty_frame = pd.DataFrame(
            columns=df.columns,
            index=range(data_lastrow - blankrows, data_lastrow),
            data=None,
        )
        return empty_frame

    @property
    def write_engine(self):
        if self.mode == "a":
            return "openpyxl"
        else:
            return "xlsxwriter"

    def write(self):
        if self.mode != "r":
            for sheet_name, sheet_deet in self.sheets.items():
                if sheet_deet["mode"] not in "r":
                    df = sheet_deet["df"]
                    if df.empty:
                        raise Exception(
                            f"cannot write empty dataframe; {sheet_name}: {df}"
                        )
            if self.mode == "w":
                with pd.ExcelWriter(
                    self.file_io, engine=self.write_engine, mode=self.mode
                ) as writer:
                    for sheet_name, sheet_deet in self.sheets.items():
                        df = sheet_deet["df"]
                        to_excel = sheet_deet["to_excel"]
                        if to_excel:
                            kwargs = to_excel.model_dump(exclude_none=True)
                        else:
                            kwargs = {}
                        df.to_excel(
                            writer, index=False, sheet_name=sheet_name, **kwargs
                        )
                    for sheet in writer.sheets.values():
                        sheet.autofit(500)
            else:  # self.mode == "a"
                sheet_exists = set()
                for sheet_deet in self.sheets.values():
                    if sheet_deet["mode"] != "r":
                        sheet_exists.add(sheet_deet["if_sheet_exists"])
                for sheet_exist in sheet_exists:
                    with pd.ExcelWriter(
                        self.file_io,
                        engine=self.write_engine,
                        mode=self.mode,
                        if_sheet_exists=sheet_exist,
                    ) as writer:
                        for sheet_name, sheet_deet in self.sheets.items():
                            if (
                                sheet_deet["mode"] != "r"
                                and sheet_deet["if_sheet_exists"] == sheet_exist
                            ):
                                df = sheet_deet["df"]
                                to_excel = sheet_deet["to_excel"]
                                if sheet_deet["mode"] == "a":
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
                                    sheet_name=sheet_name,
                                    header=header,
                                    startrow=sheet_deet["startrow"],
                                    **kwargs,
                                )

            with open(self.url, "wb") as write_file:
                self.file_io.seek(0)
                write_file.write(self.file_io.getbuffer())

    @property
    def any_empty_frames(self):
        for df_io in self.children:
            if df_io.mode not in "r":
                if df_io.df.empty:
                    print(f"cannot write empty dataframe; {df_io.name}: {df_io.df}")
                    return True
        return False

    def write2(self):
        if self.mode != "r":
            if self.any_empty_frames:
                raise Exception("Cannot write empty dataframe")
            for df_io in self.children:
                df_io.write()
                # df_io.
                # TODO this is pd persist
                # self.df_dict[df_io.name] = df_io.open_df
            self.persist()

    def persist(self):
        # if self.mode == "a":
        #     el.open_dfs[self.df_id] = el.append_into([self.open_df, self.df])
        # elif self.mode == "w":
        #     el.open_dfs[self.df_id] = self.df

        if self.mode == "w":
            with pd.ExcelWriter(
                self.file_io, engine=self.write_engine, mode=self.mode
            ) as writer:
                # for df_io in self.children:
                # for sheet_name, sheet_deet in self.sheets.items():
                for df_io in self.children:
                    print(f"writing {df_io.open_df}")
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
            # for sheet_deet in self.sheets.values():
            for df_io in self.children:
                if df_io.mode != "r":
                    sheet_exists.add(df_io.if_sheet_exists)
            for sheet_exist in sheet_exists:
                print(f"appending {df_io.open_df}")
                with pd.ExcelWriter(
                    self.file_io,
                    engine=self.write_engine,
                    mode=self.mode,
                    if_sheet_exists=sheet_exist,
                ) as writer:
                    # for sheet_name, sheet_deet in self.sheets.items():
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

    def set_new_sheet_df(self, sheet_name, df, kwargs):
        self.sheets[sheet_name] = {
            "startrow": 0,
            "mode": "w",
            "if_sheet_exists": "replace",
            "df": df,
            "to_excel": kwargs,
        }

    def set_new_sheet_df2(self, sheet_name, df, kwargs):
        if self.has_child(sheet_name):
            c = self.get_child(sheet_name)
            c.startrow = 0
            c.mode = "w"
            c.df = df
            c.to_excel = kwargs
        else:
            c = ExcelSheetIO(
                startrow=0,
                mode="w",
                df=df,
                to_excel=kwargs,
                parent=self,
                name=sheet_name,
            )

    def set_sheet_df(self, sheet_name, df, if_exists, kwargs):
        if sheet_name in self.sheets:
            if self.sheets[sheet_name]["mode"] == "r":
                if if_exists == "fail":
                    raise Exception("Failing: sheet already exists")
                elif if_exists == "replace":
                    self.set_new_sheet_df(sheet_name, df, kwargs)
                    self.set_new_sheet_df2(sheet_name, df, kwargs)

                elif if_exists == "append":
                    # ensures alignment of columns with target
                    df0 = self.pull_sheet_structure(sheet_name)
                    df = el.append_into([df0, df])

                    # this dataframe contains only the appended rows
                    # thus avoiding overwriting existing data/formats of sheet
                    self.sheets[sheet_name]["if_sheet_exists"] = "overlay"
                    self.sheets[sheet_name]["mode"] = "a"
                elif if_exists == "truncate":
                    df0 = self.pull_sheet_structure(sheet_name)
                    blank_row_count = len(self.sheets[sheet_name]["df"]) - len(df)

                    df1 = self.pull_bottom_blanks(sheet_name, blank_row_count)
                    df = el.append_into([df0, df, df1])
                    self.sheets[sheet_name]["if_sheet_exists"] = "overlay"
                    self.sheets[sheet_name]["mode"] = "a"
                    self.sheets[sheet_name]["startrow"] = 1
                else:
                    raise Exception(f"if_exists value {if_exists} not supported")
            else:
                df0 = self.sheets[sheet_name]["df"]
                df = pd.concat([df0, df])
        else:
            self.set_new_sheet_df(sheet_name, df, kwargs)
            self.set_new_sheet_df2(sheet_name, df, kwargs)
        self.sheets[sheet_name]["df"] = df

    def fetch_df_io(self, df_name, df) -> ExcelSheetIO:
        if not self.has_child(df_name):
            self.add_child(ExcelSheetIO(df=df, parent=self, name=df_name, mode="w"))
        return self.get_child(df_name)

    def set_sheet_df2(self, sheet_name, df, if_exists, kwargs):
        df_io = self.fetch_df_io(df_name=sheet_name, df=df)
        df_io.set_df(df_name=sheet_name, df=df, if_exists=if_exists)
        if if_exists == "truncate":
            df_io.truncate = True

    def close(self):
        for df_io in self.children:
            df_io.close()
        self.file_io.close()
        del el.open_files[self.url]

    @cached_property
    def mode(self):
        if len(self.sheets) == 0:
            return "r"
        elif self.create_or_replace:
            return "w"
        else:
            for deet in self.sheets.values():
                if deet["mode"] in ("a", "w"):
                    return "a"
        return "r"
