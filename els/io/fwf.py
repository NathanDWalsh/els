from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

from els._typing import IfExistsLiteral, KWArgsIO

from .base import (
    ContainerReaderABC,
    FrameABC,
    FrameModeLiteral,
)


# class FWFFrame(FrameABC["FWFContainer"]):
class FWFFrame(FrameABC):
    def __init__(
        self,
        name: str,
        parent: FWFContainer,
        if_exists: IfExistsLiteral = "fail",
        mode: FrameModeLiteral = "s",
        df: pd.DataFrame = pd.DataFrame(),
        kwargs_pull: Optional[KWArgsIO] = None,
    ):
        super().__init__(
            df=df,
            name=name,
            parent=parent,
            mode=mode,
            if_exists=if_exists,
            kwargs_pull=kwargs_pull,
        )

    def _read(self, kwargs: KWArgsIO):
        if self.kwargs_pull != kwargs:
            self.df: pd.DataFrame = pd.read_fwf(self.parent.url, **kwargs)
            self.kwargs_pull = kwargs


# class FWFContainer(ContainerReaderABC[FWFFrame]):
class FWFContainer(ContainerReaderABC):
    def __init__(
        self,
        url: str,
        replace: bool = False,
    ):
        super().__init__(FWFFrame, url)

    @property
    def create_or_replace(self):
        return False

    def _children_init(self):
        self.children = [
            FWFFrame(
                name=Path(self.url).stem,
                parent=self,
            ),
        ]

    def persist(self):
        pass  # not supported

    def close(self):
        pass  # not required / closes after read
