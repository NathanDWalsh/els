from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

from .base import ContainerReaderABC, FrameABC, KWArgsIO


class FWFFrame(FrameABC):
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

    def _read(self, kwargs: Optional[KWArgsIO] = None):
        if kwargs is None:
            assert self.kwargs_pull
            kwargs = self.kwargs_pull
        if self.kwargs_pull != kwargs:
            self.df = pd.read_fwf(self.parent.url, **kwargs)  # type: ignore
            self.kwargs_pull = kwargs


class FWFContainer(ContainerReaderABC):
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
