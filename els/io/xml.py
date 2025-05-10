from __future__ import annotations

import os
from io import StringIO
from pathlib import Path

import pandas as pd

import els.core as el

from .base import (
    ContainerWriterABC,
    FrameABC,
    FrameModeLiteral,
    IfExistsLiteral,
    append_into,
    get_column_frame,
)


class XMLFrame(FrameABC["XMLContainer"]):
    def __init__(
        self,
        name,
        parent,
        if_exists: IfExistsLiteral = "fail",
        mode: FrameModeLiteral = "s",
        df=pd.DataFrame(),
        kwargs_pull=None,
        kwargs_push=None,
    ) -> None:
        super().__init__(
            df=df,
            name=name,
            parent=parent,
            mode=mode,
            if_exists=if_exists,
            kwargs_pull=kwargs_pull,
        )
        self.kwargs_push = kwargs_push or {}

    # TODO test sample scenarios
    # TODO sample should not be optional since it is always called by super.read()
    def _read(self, kwargs):
        # if kwargs is None:
        #     kwargs = self.kwargs_pull
        if self.mode in ("s") or (self.kwargs_pull != kwargs):
            if "nrows" in kwargs:
                kwargs.pop("nrows")
            self.parent.file_io.seek(0)
            self.df = pd.read_xml(
                StringIO(self.parent.file_io.getvalue().decode("utf-8")), **kwargs
            )
            self.kwargs_pull = kwargs


class XMLContainer(ContainerWriterABC[XMLFrame]):
    def __init__(self, url, replace=False) -> None:
        super().__init__(XMLFrame, url, replace)

    @property
    def create_or_replace(self) -> bool:
        if self.replace or not os.path.isfile(self.url):
            return True
        else:
            return False

    def _children_init(self) -> None:
        self.file_io = el.fetch_file_io(self.url, replace=False)
        self.children = [
            XMLFrame(
                name=Path(self.url).stem,
                parent=self,
            )
        ]

    def persist(self) -> None:
        if self.mode in ("w", "a"):
            self.file_io = el.fetch_file_io(self.url)
            # loop not required, only one child in XML
            for df_io in self:
                df = df_io.df_target
                kwargs = df_io.kwargs_push or {}
                # TODO: relevant for XML?
                # if isinstance(df.columns, pd.MultiIndex):
                #     df = multiindex_to_singleindex(df)
                if df_io.if_exists == "truncate":
                    self.file_io.seek(0)
                    stringit = StringIO(self.file_io.getvalue().decode("utf-8"))
                    for_append = pd.read_xml(stringit)
                    self.file_io.seek(0)
                    df = append_into([get_column_frame(for_append), df])

                if df_io.if_exists == "append" and len(self.file_io.getbuffer()):
                    self.file_io.seek(0)
                    stringit = StringIO(self.file_io.getvalue().decode("utf-8"))
                    for_append = pd.read_xml(stringit)
                    self.file_io.seek(0)
                    df = append_into([for_append, df])

                df.to_xml(
                    self.file_io,
                    index=False,
                    **kwargs,
                )
                self.file_io.truncate()
            with open(self.url, "wb") as write_file:
                self.file_io.seek(0)
                write_file.write(self.file_io.getbuffer())

    def close(self) -> None:
        self.file_io.close()
        del el.io_files[self.url]
