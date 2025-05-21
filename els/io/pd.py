from __future__ import annotations

from typing import TYPE_CHECKING

import els.core as el

from .base import ContainerWriterABC, FrameABC

if TYPE_CHECKING:
    from els._typing import KWArgsIO


class DFFrame(FrameABC):
    parent: DFContainer  # for mypy

    def _read(self, kwargs: KWArgsIO) -> None:
        self.df = self.parent.df_dict[self.name]
        self.df_target = self.parent.df_dict[self.name]


class DFContainer(ContainerWriterABC[DFFrame]):
    def __init__(
        self,
        url: str,
        replace: bool = False,
    ):
        super().__init__(DFFrame, url, replace)

    def _children_init(self) -> None:
        self.df_dict = el.fetch_df_dict(self.url)
        for name in self.df_dict.keys():
            for name in self.df_dict.keys():
                self.children.append(
                    DFFrame(
                        name=name,
                        parent=self,
                    )
                )

    def persist(self) -> None:
        self.df_dict = el.fetch_df_dict(self.url)
        for df_io in self:
            if df_io.mode in ("a", "w"):
                self.df_dict[df_io.name] = df_io.df_target

    def close(self) -> None:
        pass
        # no closing operations required for dataframe
