from __future__ import annotations

import os
from pathlib import Path
from typing import Generator

import pandas as pd

import els.config as ec
import els.core as el

from .base import ContainerWriterABC, FrameABC, multiindex_to_singleindex


class CSVFrame(FrameABC):
    def __init__(
        self,
        name,
        parent,
        if_exists="fail",
        mode="s",
        df=pd.DataFrame(),
        # startrow=0,
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
        # TODO: maybe use skiprows instead?
        # self._startrow = startrow
        # self.kw_for_pull = kw_for_pull
        self.kw_for_push: ec.ToExcel = kw_for_push
        self.clean_last_column = False

    @property
    def parent(self) -> CSVContainer:
        return super().parent

    @parent.setter
    def parent(self, v):
        FrameABC.parent.fset(self, v)

    # TODO test sample scenarios
    # TODO sample should not be optional since it is always called by super.read()
    def _read(self, kwargs: dict):
        if kwargs.get("nrows") and kwargs.get("skipfooter"):
            del kwargs["nrows"]
        if "clean_last_column" in kwargs:
            self.clean_last_column = kwargs.pop("clean_last_column")
        if not kwargs:
            kwargs = self.kw_for_pull
        if self.mode in ("r", "s") and self.kw_for_pull != kwargs:
            self.df = pd.read_csv(self.parent.file_io, **kwargs)
            # check if last column is unnamed
            if (
                self.clean_last_column
                and isinstance(self.df.columns[-1], str)
                and self.df.columns[-1].startswith("Unnamed")
            ):
                # check if the last column is all null
                if self.df[self.df.columns[-1]].isnull().all():
                    # drop the last column
                    self.df = self.df.drop(self.df.columns[-1], axis=1)

            self.kw_for_pull = kwargs


class CSVContainer(ContainerWriterABC):
    def __init__(self, url, replace=False):
        super().__init__(CSVFrame, url, replace)

    def __iter__(self) -> Generator[CSVFrame, None, None]:
        for child in super().children:
            yield child

    @property
    def create_or_replace(self):
        if self.replace or not os.path.isfile(self.url):
            return True
        else:
            return False

    def _children_init(self):
        self.file_io = el.fetch_file_io(self.url, replace=self.create_or_replace)
        CSVFrame(
            name=Path(self.url).stem,
            parent=self,
        )

    def persist(self):
        if self.mode in ("w", "a"):
            self.file_io = el.fetch_file_io(self.url)
            # loop not required, only one child in csv
            for df_io in self:
                df = df_io.df_target
                to_csv = df_io.kw_for_push
                if to_csv:
                    kwargs = to_csv.model_dump(exclude_none=True)
                else:
                    kwargs = {}
                # TODO integrate better into write method?
                if isinstance(df.columns, pd.MultiIndex):
                    df = multiindex_to_singleindex(df)

                if df_io.if_exists == "truncate":
                    #     df_io.mode = "w"
                    self.file_io.seek(0)
                df.to_csv(
                    self.file_io,
                    index=False,
                    mode=df_io.mode,
                    # header=False,
                    header=True if df_io.mode == "w" else False,
                    **kwargs,
                )
                self.file_io.truncate()
            with open(self.url, "wb") as write_file:
                # self.file_io.seek(0)
                write_file.write(self.file_io.getbuffer())

    def close(self):
        self.file_io.close()
        del el.io_files[self.url]
