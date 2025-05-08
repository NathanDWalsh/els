from __future__ import annotations

from pathlib import Path

import pandas as pd

from .base import ContainerReaderABC, FrameABC


class FWFFrame(FrameABC["FWFContainer"]):
    def __init__(
        self,
        name,
        parent,
        if_exists="fail",
        mode="s",
        df=pd.DataFrame(),
        kwargs_pull=None,
    ):
        super().__init__(
            df=df,
            name=name,
            parent=parent,
            mode=mode,
            if_exists=if_exists,
            kwargs_pull=kwargs_pull,
        )

    def _read(self, kwargs):
        if self.kwargs_pull != kwargs:
            self.df = pd.read_fwf(self.parent.url, **kwargs)
            self.kwargs_pull = kwargs


class FWFContainer(ContainerReaderABC[FWFFrame]):
    def __init__(self, url, replace=False):
        super().__init__(FWFFrame, url)

    @property
    def create_or_replace(self):
        return False

    def _children_init(self):
        self.children = (
            FWFFrame(
                name=Path(self.url).stem,
                parent=self,
            ),
        )

    def persist(self):
        pass  # not supported

    def close(self):
        pass  # not required / closes after read
