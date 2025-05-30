from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Optional

import pandas as pd

from .base import ContainerReaderABC, FrameABC

if TYPE_CHECKING:
    from els._typing import FrameModeLiteral, IfExistsLiteral, KWArgsIO


class FWFFrame(FrameABC):
    parent: FWFContainer  # for mypy

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

    def _read(self, kwargs: KWArgsIO) -> None:
        if self.kwargs_pull != kwargs:
            assert kwargs.pop("chunksize", None) is None
            assert not kwargs.pop("iterator", False)
            self.df: pd.DataFrame = pd.read_fwf(
                self.parent.url, chunksize=None, iterator=False, **kwargs
            )
            self.kwargs_pull = kwargs


class FWFContainer(ContainerReaderABC[FWFFrame]):
    # class FWFContainer(ContainerReaderABC):
    def __init__(
        self,
        url: str,
        replace: bool = False,
    ):
        super().__init__(FWFFrame, url)

    @property
    def create_or_replace(self) -> bool:
        return False

    def _children_init(self) -> None:
        self.children = [
            FWFFrame(
                name=Path(self.url).stem,
                parent=self,
            ),
        ]

    def persist(self) -> None:
        pass  # not supported

    def close(self) -> None:
        pass  # not required / closes after read
