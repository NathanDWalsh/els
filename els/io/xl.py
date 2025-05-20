from __future__ import annotations

import io
import os
from typing import Any, Literal, Optional

import pandas as pd
from python_calamine import CalamineWorkbook, SheetTypeEnum, SheetVisibleEnum

import els.core as el
from els._typing import IfSheetExistsLiteral, KWArgsIO

from .base import (
    ContainerWriterABC,
    FrameABC,
    FrameModeLiteral,
    IfExistsLiteral,
    multiindex_to_singleindex,
)

# TODO: add/test support for other workbook types


def get_sheet_names(
    xl_io: io.BytesIO,
    sheet_states: list[SheetVisibleEnum] = [SheetVisibleEnum.Visible],
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


def get_sheet_row(
    xl_io: io.BytesIO,
    sheet_name: str,
    row_index: int,
) -> Optional[list[Any]]:
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


# class XLFrame(FrameABC["XLContainer"]):
class XLFrame(FrameABC):
    parent: XLContainer  # for mypy

    def __init__(
        self,
        name: str,
        parent: XLContainer,
        if_exists: IfExistsLiteral = "fail",
        mode: FrameModeLiteral = "s",
        df: pd.DataFrame = pd.DataFrame(),
        startrow: int = 0,
        kwargs_pull: Optional[KWArgsIO] = None,
        kwargs_push: Optional[KWArgsIO] = None,
    ) -> None:
        super().__init__(
            df=df,
            name=name,
            parent=parent,
            mode=mode,
            if_exists=if_exists,
            kwargs_pull=kwargs_pull,
        )
        self._startrow = startrow
        self.kwargs_push = kwargs_push or {}
        self.header_cell: Optional[str] = None
        self.footer_cell: Optional[str] = None

    @property
    def if_sheet_exists(self) -> IfSheetExistsLiteral:
        if self.mode == "a":
            return "overlay"
        elif self.mode == "w":
            return "replace"
        else:
            raise Exception("Invalid mode for if_sheet_exists")

    @property
    def startrow(self) -> int:
        if self.if_exists == "truncate" or self.mode == "w":
            # consider changing 0 to skiprows value if exists?
            # kwargs_push['skiprows']
            # TODO: test skiprow and truncate combinations
            return 0
        else:
            return self._startrow

    @startrow.setter
    def startrow(self, v: int) -> None:
        self._startrow = v

    def _read(self, kwargs: KWArgsIO) -> None:
        if kwargs.get("nrows") and kwargs.get("skipfooter"):
            del kwargs["nrows"]
        capture_header = kwargs.pop("capture_header", False)
        capture_footer = kwargs.pop("capture_footer", False)
        if self.kwargs_pull != kwargs:
            self.df = pd.read_excel(  # type: ignore
                self.parent.file_io,
                engine=kwargs.pop("engine", "calamine"),
                sheet_name=kwargs.pop("sheet_name", self.name),
                **kwargs,
            )
            assert isinstance(self.df, pd.DataFrame)

            skiprows = kwargs.get("skiprows", 0)
            assert self.name
            if skiprows > 0 and capture_header:
                if self.header_cell is None:
                    self.header_cell = get_header_cell(
                        self.parent.file_io,
                        self.name,
                        nrows=skiprows,
                    )
                self.df["_header"] = self.header_cell

            skipfooter = kwargs.get("skipfooter", 0)
            if skipfooter > 0 and capture_footer:
                if self.footer_cell is None:
                    self.footer_cell = get_footer_cell(
                        self.parent.file_io,
                        self.name,
                        nrows=skipfooter,
                    )
                self.df["_footer"] = self.footer_cell

            self.kwargs_pull = kwargs


class XLContainer(ContainerWriterABC[XLFrame]):
    def __init__(
        self,
        url: str,
        replace: bool = False,
    ):
        super().__init__(XLFrame, url, replace)

    @property
    def create_or_replace(self) -> bool:
        if self.replace or not os.path.isfile(self.url):
            return True
        else:
            return False

    @property
    def write_engine(self) -> Literal["openpyxl", "xlsxwriter"]:
        if self.mode == "a":
            return "openpyxl"
        else:
            return "xlsxwriter"

    def _children_init(self) -> None:
        self.file_io = el.fetch_file_io(self.url)
        with CalamineWorkbook.from_filelike(self.file_io) as workbook:
            self.children = [
                XLFrame(
                    startrow=workbook.get_sheet_by_name(sheet.name).total_height + 1,
                    name=sheet.name,
                    parent=self,
                )
                for sheet in workbook.sheets_metadata
                if (sheet.visible in [SheetVisibleEnum.Visible])
                and (sheet.typ == SheetTypeEnum.WorkSheet)
            ]

    def persist(self) -> None:
        if self.mode == "w":
            self.file_io = el.fetch_file_io(self.url, replace=True)
            with pd.ExcelWriter(
                self.file_io, engine=self.write_engine, mode=self.mode
            ) as writer:
                for df_io in self:
                    df = df_io.df_target
                    kwargs = df_io.kwargs_push
                    # TODO integrate better into write method?
                    if isinstance(df.columns, pd.MultiIndex):
                        df = multiindex_to_singleindex(df)
                    df.to_excel(
                        writer,
                        index=False,
                        sheet_name=df_io.name,
                        **kwargs,
                    )
                for sheet in writer.sheets.values():
                    sheet.autofit(500)
        elif self.mode == "a":
            sheet_exists: set[IfSheetExistsLiteral] = set()
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
                            kwargs = df_io.kwargs_push
                            if df_io.mode == "a":
                                header = False
                            else:
                                header = True
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

    def close(self) -> None:
        self.file_io.close()
        del el.io_files[self.url]
