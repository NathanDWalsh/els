import els.core as el

from .base import ContainerWriterABC, FrameABC


class DFFrame(FrameABC):
    def _read(self, kwargs={}):
        self.df = self.parent.df_dict[self.name]
        self.df_target = self.parent.df_dict[self.name]


class DFContainer(ContainerWriterABC):
    def __init__(
        self,
        url,
        replace=False,
    ):
        # self.child_class = DataFrameIO
        # self.url = url
        super().__init__(DFFrame, url, replace)

    def __repr__(self):
        return f"DataFrameDictIO({(self.url, self.replace)})"

    def _children_init(self) -> None:
        self.df_dict = el.fetch_df_dict(self.url)
        for name in self.df_dict.keys():
            DFFrame(
                name=name,
                parent=self,
            )

    def persist(self):
        self.df_dict = el.fetch_df_dict(self.url)
        for df_io in self:
            if df_io.mode in ("a", "w"):
                self.df_dict[df_io.name] = df_io.df_target

    def close(self):
        pass
        # no closing operations required for dataframe
