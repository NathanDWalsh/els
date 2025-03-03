import os
from functools import cached_property

import pandas as pd
from python_calamine import CalamineWorkbook, SheetTypeEnum, SheetVisibleEnum

import els.core as el


def get_sheet_names(xlIO, sheet_states: list = [SheetVisibleEnum.Visible]) -> list[str]:
    xlIO.seek(0)
    with CalamineWorkbook.from_filelike(xlIO) as workbook:
        worksheet_names = [
            sheet.name
            for sheet in workbook.sheets_metadata
            if (sheet.visible in sheet_states)
            and (sheet.typ == SheetTypeEnum.WorkSheet)
        ]
        return worksheet_names


def get_sheet_height(xlIO, sheet_name: str) -> int:
    xlIO.seek(0)
    with CalamineWorkbook.from_filelike(xlIO) as workbook:
        if sheet_name in workbook.sheet_names:
            return workbook.get_sheet_by_name(sheet_name).total_height
        else:
            return None


def get_sheet_row(xlIO, sheet_name: str, row_index: int) -> list:
    xlIO.seek(0)
    with CalamineWorkbook.from_filelike(xlIO) as workbook:
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
        self.fileIO = el.fetch_file_io(self.url, replace=self.replace)
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
            self.fileIO.seek(0)
            with CalamineWorkbook.from_filelike(self.fileIO) as workbook:
                res = {
                    sheet.name: {
                        "startrow": workbook.get_sheet_by_name(sheet.name).total_height
                        + 1,
                        "mode": "r",
                        "kwargs": {},
                    }
                    for sheet in workbook.sheets_metadata
                    if (sheet.visible in sheet_states)
                    and (sheet.typ == SheetTypeEnum.WorkSheet)
                }
                return res

    def pull_sheet(self, **kwargs):
        sheet_name = kwargs["sheet_name"]
        if sheet_name in self.sheets:
            sheet = self.sheets[sheet_name]
            if sheet["mode"] == "r" and sheet["kwargs"] != kwargs:
                sheet["df"] = pd.read_excel(self.fileIO, engine="calamine", **kwargs)
                sheet["kwargs"] = kwargs
            return sheet["df"]
        else:
            raise Exception(f"sheet not found: {sheet_name}")

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
                with pd.ExcelWriter(self.fileIO, mode=self.mode) as writer:
                    for sheet_name, sheet_deet in self.sheets.items():
                        df = sheet_deet["df"]
                        # TODO: startrow=start_row, header=False
                        df.to_excel(writer, index=False, sheet_name=sheet_name)
            else:
                sheet_exists = set()
                for sheet_deet in self.sheets.values():
                    sheet_exists.add(sheet_deet["if_sheet_exists"])
                for sheet_exist in sheet_exists:
                    for sheet_name, sheet_deet in self.sheets.items():
                        if sheet_deet["if_sheet_exists"] == sheet_exist:
                            with pd.ExcelWriter(
                                self.fileIO, mode=self.mode, if_sheet_exists=sheet_exist
                            ) as writer:
                                # TODO: startrow=start_row, header=False
                                df.to_excel(writer, index=False, sheet_name=sheet_name)
                                self.mode = "a"
            with open(self.url, "wb") as write_file:
                self.fileIO.seek(0)
                write_file.write(self.fileIO.getbuffer())

    def set_new_sheet_df(self, sheet_name, df):
        self.sheets[sheet_name] = {
            "startrow": 1,  # <- one-based ?
            "mode": "w",
            "if_sheet_exists": "replace",  # <- redundant?
            "df": df,
        }

    def set_sheet_df(self, sheet_name, df, if_exists):
        if sheet_name in self.sheets:
            if self.sheets[sheet_name]["mode"] == "r":
                if if_exists.value == "fail":
                    raise Exception("Failing: sheet already exists")
                elif if_exists.value == "replace":
                    self.set_new_sheet_df(sheet_name, df)
                elif if_exists.value == "append":
                    # TODO: ensure alignment of columns?
                    self.sheets[sheet_name]["if_sheet_exists"] = "overlay"
                else:
                    # TODO: support truncate?
                    raise Exception(f"if_exists value {if_exists} not supported")
            else:
                # TODO: handle appending two dataframes
                pass
        else:
            self.set_new_sheet_df(sheet_name, df)
        self.sheets[sheet_name]["df"] = df

    def close(self):
        self.fileIO.close()
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
