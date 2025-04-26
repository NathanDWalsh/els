from __future__ import annotations

import os
from pathlib import Path
from typing import Generator

import pandas as pd

import els.config as ec
import els.core as el

from . import base as eio


class XMLFrame(eio.FrameABC):
    def __init__(
        self,
        name,
        parent,
        if_exists="fail",
        mode="s",
        df=pd.DataFrame(),
        # startrow=0,
        kw_for_pull={},
        kw_for_push={},
    ) -> None:
        super().__init__(
            df=df,
            name=name,
            parent=parent,
            mode=mode,
            if_exists=if_exists,
        )
        # TODO: maybe use skiprows instead?
        # self._startrow = startrow
        self.kw_for_pull = kw_for_pull
        self.kw_for_push: ec.ToXML = kw_for_push
        # self.clean_last_column = False
        # print(f"BEFORE READ:{self.df}")
        # self.read()
        # print(f"AFTER READ:{self.df}")

    @property
    def parent(self) -> XMLContainer:
        return super().parent

    @parent.setter
    def parent(self, v):
        eio.FrameABC.parent.fset(self, v)

    # def append(self, df):
    #     print("DO NOT TRUNCATE")
    #     self._append(df, truncate_first=True)

    # def _append(self, df, truncate_first=False):
    #     self.read()

    #     self.df = pd.read_xml(self.parent.file_io)
    #     self.df_target = self.df
    # print(self.df)
    # print("READ BEFORE APPEND")
    # self.read()
    # print("BEFORE APPEND")
    # print(self.df)
    # self.df = eio.append_into([self.df, df])
    # print("AFTER APPEND")
    # print(self.df)

    # def _build(self, df):
    #     df = eio.get_column_frame(df)
    #     # self.read()
    #     self.df_target = eio.append_into([df, self.df_target])
    #     self.df = self.df_target
    #     return df

    # self.read()
    # return self.df
    # print("JUST READ")
    # print(self.df)

    # TODO test sample scenarios
    # TODO sample should not be optional since it is always called by super.read()
    def _read(self, kwargs: dict):
        # print(f"READ,mode:{self.mode}")
        if not kwargs:
            kwargs = self.kw_for_pull
        # print(f"kwargs:{[self.kw_for_pull, kwargs]}")
        print("FUCK")
        if self.mode in ("r", "s", "m") and (self.kw_for_pull != kwargs):
            # if self.mode in ("s", "m") or self.kw_for_pull != kwargs:
            if "nrows" in kwargs:
                kwargs.pop("nrows")
                self.parent.file_io.seek(0)
            # print(f"READ,mode:{kwargs}")
            # print(f"READ,mode:{self.parent.file_io.name}")
            print("READ XML")
            self.df = pd.read_xml(self.parent.file_io, **kwargs)
            print(f"READ {self.df}")
            # self.df_target = self.df
            # print(f"df: {self.df}")
            self.kw_for_pull = kwargs


class XMLContainer(eio.ContainerWriterABC):
    def __init__(self, url, replace=False):
        super().__init__(XMLFrame, url, replace)

    def __iter__(self) -> Generator[XMLFrame, None, None]:
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
        XMLFrame(
            name=Path(self.url).stem,
            parent=self,
        )

    def persist(self):
        if self.mode in ("w", "a"):
            self.file_io = el.fetch_file_io(self.url)
            # loop not required, only one child in XML
            for df_io in self:
                df = df_io.df_target
                to_xml = df_io.kw_for_push
                if to_xml:
                    kwargs = to_xml.model_dump(exclude_none=True)
                else:
                    kwargs = {}
                # TODO: relevant for XML?
                # if isinstance(df.columns, pd.MultiIndex):
                #     df = eio.multiindex_to_singleindex(df)

                if df_io.if_exists == "truncate":
                    #     df_io.mode = "w"
                    # self.file_io.seek(0)
                    pass
                df.to_xml(
                    self.file_io,
                    index=False,
                    **kwargs,
                )
                self.file_io.truncate()
            with open(self.url, "wb") as write_file:
                self.file_io.seek(0)
                write_file.write(self.file_io.getbuffer())

    def close(self):
        self.file_io.close()
        del el.io_files[self.url]
