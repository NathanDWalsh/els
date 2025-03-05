import os
from functools import cached_property

import pandas as pd
from python_calamine import CalamineWorkbook, SheetTypeEnum, SheetVisibleEnum

import els.core as el


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


class ExcelIO:
    def __init__(self, url, replace=False):
        self.url = url
        self.replace = replace

        # load file and sheets
        self.file_io = el.fetch_file_io(self.url, replace=self.replace)
        self.sheets = self.get_sheet_deets()

    @cached_property
    def create_or_replace(self):
        if self.replace or not os.path.isfile(self.url):
            return True
        else:
            return False

    def get_sheet_deets(self, sheet_states: list = [SheetVisibleEnum.Visible]) -> dict:
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
                        "kwargs": {},
                        "read_excel": {},
                        "to_excel": {},
                    }
                    for sheet in workbook.sheets_metadata
                    if (sheet.visible in sheet_states)
                    and (sheet.typ == SheetTypeEnum.WorkSheet)
                }
                return res

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

    def pull_sheet_structure(self, sheet_name, sample_rows=100):
        kwargs = {}
        kwargs["sheet_name"] = sheet_name
        kwargs["nrows"] = sample_rows
        df = self.pull_sheet(kwargs)
        empty_frame = pd.DataFrame(columns=df.columns, index=None, data=None)
        empty_frame = empty_frame.astype(df.dtypes)
        return empty_frame

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
                                self.mode = "a"

            with open(self.url, "wb") as write_file:
                self.file_io.seek(0)
                write_file.write(self.file_io.getbuffer())

    def set_new_sheet_df(self, sheet_name, df, kwargs):
        self.sheets[sheet_name] = {
            "startrow": 0,
            "mode": "w",
            "if_sheet_exists": "replace",  # <- redundant?
            "df": df,
            "to_excel": kwargs,
        }

    def set_sheet_df(self, sheet_name, df, if_exists, kwargs):
        if sheet_name in self.sheets:
            if self.sheets[sheet_name]["mode"] == "r":
                if not isinstance(if_exists, str):
                    if_exists = if_exists.value
                if if_exists == "fail":
                    raise Exception("Failing: sheet already exists")
                elif if_exists == "replace":
                    self.set_new_sheet_df(sheet_name, df, kwargs)
                elif if_exists == "append":
                    # ensures alignment of columns with target
                    df0 = self.pull_sheet_structure(sheet_name)
                    df = pd.concat([df0, df], join="inner")

                    self.sheets[sheet_name]["if_sheet_exists"] = "overlay"
                    self.sheets[sheet_name]["mode"] = "a"
                elif if_exists == "append":
                    df0 = self.pull_sheet_structure(sheet_name)
                    df = pd.concat([df0, df], join="inner")
                elif if_exists == "truncate":
                    df0 = self.pull_sheet_structure(sheet_name)
                    blank_row_count = len(self.sheets[sheet_name]["df"]) - len(df)
                    df1 = self.pull_bottom_blanks(sheet_name, blank_row_count)
                    df = pd.concat([df0, df, df1], join="inner")

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
        self.sheets[sheet_name]["df"] = df

    def close(self):
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
