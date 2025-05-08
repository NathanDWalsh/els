from __future__ import annotations

import csv
import io
import os
from pathlib import Path
from typing import Optional

import pandas as pd

import els.core as el
from els.typing import IfExistsLiteral, KWArgsIO

from .base import (
    ContainerWriterABC,
    FrameABC,
    FrameModeLiteral,
    multiindex_to_singleindex,
)


def get_header_cell(
    csv_io: io.BytesIO,
    nrows: int,
    sep: str,
) -> str:
    csv_io.seek(0)
    # TODO different encodings?
    # TODO better scope the sio io.StringIO object
    sio = io.StringIO(csv_io.getvalue().decode("utf-8"))
    reader = csv.reader(sio, delimiter=sep)
    rows = [next(reader) for _ in range(nrows)]
    return str(rows)


def get_footer_cell(
    csv_io: io.BytesIO,
    nrows: int,
    sep: str,
) -> str:
    csv_io.seek(0)
    # TODO different encodings?
    # TODO better scope the sio io.StringIO object
    sio = io.StringIO(csv_io.getvalue().decode("utf-8"))
    reader = csv.reader(sio, delimiter=sep)
    # TODO: read from end of file for performance
    all_rows = list(reader)
    rows = all_rows[-nrows:]
    return str(rows)


class CSVFrame(FrameABC["CSVContainer"]):
    def __init__(
        self,
        name: str,
        parent: CSVContainer,
        if_exists: IfExistsLiteral = "fail",
        mode: FrameModeLiteral = "s",
        df: pd.DataFrame = pd.DataFrame(),
        kwargs_pull: Optional[KWArgsIO] = None,
        kwargs_push: Optional[KWArgsIO] = None,
    ) -> None:
        self.header_cell: Optional[str] = None
        self.footer_cell: Optional[str] = None
        self.kwargs_push = kwargs_push or {}
        super().__init__(
            df=df,
            name=name,
            parent=parent,
            mode=mode,
            if_exists=if_exists,
            kwargs_pull=kwargs_pull,
        )

    def _read(self, kwargs):
        if "nrows" in kwargs and "skipfooter" in kwargs:
            del kwargs["nrows"]
        clean_last_column = kwargs.pop("clean_last_column", False)
        capture_header = kwargs.pop("capture_header", False)
        capture_footer = kwargs.pop("capture_footer", False)
        if self.kwargs_pull != kwargs:
            self.parent.file_io.seek(0)
            self.df = pd.read_csv(self.parent.file_io, **kwargs)
            # TODO: add tests
            if (
                clean_last_column
                and self.df.columns[-1].startswith("Unnamed")
                and self.df[self.df.columns[-1]].isnull().all()
            ):
                self.df = self.df.drop(self.df.columns[-1], axis=1)

            skiprows = kwargs.get("skiprows", 0)
            if skiprows > 0 and capture_header:
                if not self.header_cell:
                    self.header_cell = get_header_cell(
                        self.parent.file_io,
                        nrows=skiprows,
                        sep=kwargs.get("sep", ","),
                    )
                self.df["_header"] = self.header_cell

            skipfooter = kwargs.get("skipfooter", 0)
            if skipfooter > 0 and capture_footer:
                if not self.footer_cell:
                    self.footer_cell = get_footer_cell(
                        self.parent.file_io,
                        nrows=skipfooter,
                        sep=kwargs.get("sep", ","),
                    )
                self.df["_footer"] = self.footer_cell
            self.kwargs_pull = kwargs


class CSVContainer(ContainerWriterABC[CSVFrame]):
    def __init__(
        self,
        url: str,
        replace: bool = False,
    ) -> None:
        super().__init__(CSVFrame, url, replace)

    @property
    def create_or_replace(self):
        if self.replace or not os.path.isfile(self.url):
            return True
        else:
            return False

    def _children_init(self):
        self.file_io = el.fetch_file_io(self.url, replace=self.create_or_replace)
        self.children = [
            CSVFrame(
                name=Path(self.url).stem,
                parent=self,
            )
        ]

    def persist(self):
        if self.mode in ("w", "a"):
            self.file_io = el.fetch_file_io(self.url)
            # loop not required, only one child in csv
            for df_io in self:
                df = df_io.df_target
                kwargs = df_io.kwargs_push
                # TODO integrate better into write method?
                if isinstance(df.columns, pd.MultiIndex):
                    df = multiindex_to_singleindex(df)

                if df_io.if_exists == "truncate":
                    #     df_io.mode = "w"
                    self.file_io.seek(0)
                header = kwargs.pop("header", True if df_io.mode == "w" else False)
                df.to_csv(
                    self.file_io,
                    index=False,
                    mode=df_io.mode,
                    # header=False,
                    # header=True if df_io.mode == "w" else False,
                    header=header,
                    **kwargs,
                )
                self.file_io.truncate()
            with open(self.url, "wb") as write_file:
                # self.file_io.seek(0)
                write_file.write(self.file_io.getbuffer())

    def close(self):
        self.file_io.close()
        del el.io_files[self.url]
