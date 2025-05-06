from __future__ import annotations

import els.core as el

from .base import ContainerWriterABC, FrameABC


class DFFrame(FrameABC):
    def _read(self, kwargs=None) -> None:
        parent: DFContainer = self.parent  # type: ignore
        self.df = parent.df_dict[self.name]
        self.df_target = parent.df_dict[self.name]


class DFContainer(ContainerWriterABC):
    def __init__(
        self,
        url,
        replace=False,
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

    def persist(self):
        self.df_dict = el.fetch_df_dict(self.url)
        for df_io in self:
            if df_io.mode in ("a", "w"):
                self.df_dict[df_io.name] = df_io.df_target

    def close(self):
        pass
        # no closing operations required for dataframe
