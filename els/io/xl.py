from __future__ import annotations

import io
import os
from typing import Generator, Optional

import pandas as pd
from python_calamine import CalamineWorkbook, SheetTypeEnum, SheetVisibleEnum

import els.config as ec
import els.core as el

from .base import ContainerWriterABC, FrameABC, multiindex_to_singleindex


def get_sheet_names(
    xl_io: io.BytesIO,
    sheet_states: list = [SheetVisibleEnum.Visible],
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


# def get_sheet_height(
#     xl_io: io.BytesIO,
#     sheet_name: str,
# ) -> Optional[int]:
#     xl_io.seek(0)
#     with CalamineWorkbook.from_filelike(xl_io) as workbook:
#         if sheet_name in workbook.sheet_names:
#             return workbook.get_sheet_by_name(sheet_name).total_height
#         else:
#             return None


def get_sheet_row(
    xl_io: io.BytesIO,
    sheet_name: str,
    row_index: int,
) -> Optional[list]:
    xl_io.seek(0)
    with CalamineWorkbook.from_filelike(xl_io) as workbook:
        if sheet_name in workbook.sheet_names:
            return workbook.get_sheet_by_name(sheet_name).to_python(
                nrows=row_index + 1
            )[-1]
        else:
            return None


def get_header_cell(
    xl_io: io.BytesIO,
    sheet_name: str,
    nrows: int,
) -> str:
    xl_io.seek(0)
    with CalamineWorkbook.from_filelike(xl_io) as wb:
        rows = wb.get_sheet_by_name(sheet_name).to_python(
            nrows=nrows,
            skip_empty_area=False,
        )
        return str(rows)


def get_footer_cell(
    xl_io: io.BytesIO,
    sheet_name: str,
    nrows: int,
) -> str:
    xl_io.seek(0)
    with CalamineWorkbook.from_filelike(xl_io) as wb:
        rows = wb.get_sheet_by_name(sheet_name).to_python()[-nrows:]
        return str(rows)


# def get_xl_dynamic_cell_value(frame: ec.Source, add_cols):
#     for k, v in add_cols.items():
#         # check if the value is a DynamicCellValue
#         if (
#             v
#             and isinstance(v, str)
#             and v[1:].upper() in ec.DynamicCellValue.__members__.keys()
#         ):
#             row, col = v[1:].upper().strip("R").split("C")
#             row = int(row)
#             col = int(col)
#             # get the cell value corresponding to the row/col
#             add_cols[k] = get_sheet_row(xl_io.file_io, frame.sheet_name, row)[col]


class XLFrame(FrameABC):
    def __init__(
        self,
        name,
        parent,
        if_exists="fail",
        mode="s",
        df=pd.DataFrame(),
        startrow=0,
        kw_for_pull=None,
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
        self._startrow = startrow
        self.kw_for_push: ec.ToExcel = kw_for_push
        self.header_cell = None
        self.footer_cell = None

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
        if self.if_exists == "truncate" or self.mode == "w":
            # consider changing 0 to skiprows value if exists?
            # kw_for_push['skiprows']
            # TODO: test skiprow and truncate combinations
            return 0
        else:
            return self._startrow

    @startrow.setter
    def startrow(self, v):
        self._startrow = v

    @property
    def parent(self) -> XLContainer:
        return super().parent

    @parent.setter
    def parent(self, v):
        FrameABC.parent.fset(self, v)

    def _read(self, kwargs):
        if kwargs.get("nrows") and kwargs.get("skipfooter"):
            del kwargs["nrows"]
        if not kwargs:
            kwargs = self.kw_for_pull
        capture_header = kwargs.pop("capture_header", False)
        capture_footer = kwargs.pop("capture_footer", False)
        if self.mode in ("r", "s") and self.kw_for_pull != kwargs:
            if "engine" not in kwargs:
                kwargs["engine"] = "calamine"
            if "sheet_name" not in kwargs:
                kwargs["sheet_name"] = self.name
            self.df = pd.read_excel(self.parent.file_io, **kwargs)

            skiprows = kwargs.get("skiprows", 0)
            if skiprows > 0 and capture_header:
                if not self.header_cell:
                    self.header_cell = get_header_cell(
                        self.parent.file_io,
                        self.name,
                        nrows=skiprows,
                    )
                self.df["_header"] = self.header_cell

            skipfooter = kwargs.get("skipfooter", 0)
            if skipfooter > 0 and capture_footer:
                if not self.footer_cell:
                    self.footer_cell = get_footer_cell(
                        self.parent.file_io,
                        self.name,
                        nrows=skipfooter,
                    )
                self.df["_footer"] = self.footer_cell

            self.kw_for_pull = kwargs


class XLContainer(ContainerWriterABC):
    def __init__(self, url, replace=False):
        super().__init__(XLFrame, url, replace)

    def __iter__(self) -> Generator[XLFrame, None, None]:
        for child in super().children:
            yield child

    @property
    def create_or_replace(self):
        if self.replace or not os.path.isfile(self.url):
            return True
        else:
            return False

    @property
    def write_engine(self):
        if self.mode == "a":
            return "openpyxl"
        else:
            return "xlsxwriter"

    def _children_init(self):
        self.file_io = el.fetch_file_io(self.url)
        with CalamineWorkbook.from_filelike(self.file_io) as workbook:
            return [
                XLFrame(
                    startrow=workbook.get_sheet_by_name(sheet.name).total_height + 1,
                    name=sheet.name,
                    parent=self,
                )
                for sheet in workbook.sheets_metadata
                if (sheet.visible in [SheetVisibleEnum.Visible])
                and (sheet.typ == SheetTypeEnum.WorkSheet)
            ]

    def persist(self):
        if self.mode == "w":
            self.file_io = el.fetch_file_io(self.url, replace=True)
            with pd.ExcelWriter(
                self.file_io, engine=self.write_engine, mode=self.mode
            ) as writer:
                for df_io in self:
                    df = df_io.df_target
                    to_excel = df_io.kw_for_push
                    if to_excel:
                        kwargs = to_excel.model_dump(exclude_none=True)
                    else:
                        kwargs = {}
                    # TODO integrate better into write method?
                    if isinstance(df.columns, pd.MultiIndex):
                        df = multiindex_to_singleindex(df)
                    df.to_excel(writer, index=False, sheet_name=df_io.name, **kwargs)
                for sheet in writer.sheets.values():
                    sheet.autofit(500)
        elif self.mode == "a":
            sheet_exists = set()
            for df_io in self:
                if df_io.mode not in ("r", "s"):
                    sheet_exists.add(df_io.if_sheet_exists)
            for sheet_exist in sheet_exists:
                with pd.ExcelWriter(
                    self.file_io,
                    engine=self.write_engine,
                    mode=self.mode,
                    if_sheet_exists=sheet_exist,
                ) as writer:
                    for df_io in self:
                        if (
                            df_io.mode not in ("r", "s")
                            and df_io.if_sheet_exists == sheet_exist
                        ):
                            df = df_io.df_target
                            to_excel = df_io.kw_for_push
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
                                df = multiindex_to_singleindex(df)
                            df.to_excel(
                                writer,
                                index=False,
                                sheet_name=df_io.name,
                                header=header,
                                startrow=df_io.startrow,
                                **kwargs,
                            )
        # TODO: should this be nested? Ensuring mode is a or w
        with open(self.url, "wb") as write_file:
            self.file_io.seek(0)
            write_file.write(self.file_io.getbuffer())

    def close(self):
        self.file_io.close()
        del el.io_files[self.url]
