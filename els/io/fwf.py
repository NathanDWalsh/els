from __future__ import annotations

from pathlib import Path
from typing import Generator

import pandas as pd

from .base import ContainerReaderABC, FrameABC


class FWFFrame(FrameABC):
    def __init__(
        self,
        name,
        parent,
        if_exists="fail",
        mode="s",
        df=pd.DataFrame(),
        kw_for_pull=None,
    ):
        super().__init__(
            df=df,
            name=name,
            parent=parent,
            mode=mode,
            if_exists=if_exists,
            kw_for_pull=kw_for_pull,
        )

    @property
    def parent(self) -> FWFContainer:
        return super().parent

    @parent.setter
    def parent(self, v):
        FrameABC.parent.fset(self, v)

    # TODO test sample scenarios
    # TODO sample should not be optional since it is always called by super.read()
    def _read(self, kwargs: dict):
        if not kwargs:
            assert self.kw_for_pull
            kwargs = self.kw_for_pull
        if self.mode in ("r", "s") and self.kw_for_pull != kwargs:
            self.df = pd.read_fwf(self.parent.url, **kwargs)
            self.kw_for_pull = kwargs


class FWFContainer(ContainerReaderABC):
    def __init__(self, url, replace=False):
        super().__init__(FWFFrame, url)

    def __iter__(self) -> Generator[FWFFrame, None, None]:
        for child in super().children:
            yield child

    @property
    def create_or_replace(self):
        return False

    def _children_init(self):
        # self.file_io = el.fetch_file_io(self.url, replace=self.create_or_replace)
        # self.file_io.seek(0)
        FWFFrame(
            name=Path(self.url).stem,
            parent=self,
        )

    def persist(self):
        pass  # not supported

    def close(self):
        pass  # not required / closes after read
        # self.file_io.close()
        # del el.io_files[self.url]
