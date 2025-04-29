from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generator, Literal, Optional

import pandas as pd
from anytree import NodeMixin  # type: ignore

nrows_for_sampling: int = 100


def multiindex_to_singleindex(df: pd.DataFrame, separator: str = "_") -> pd.DataFrame:
    df.columns = [separator.join(map(str, col)).strip() for col in df.columns.values]  # type: ignore
    return df


def append_into(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    # appends subsequent dfs into the first df, keeping only the columns from the first
    ncols = len(dfs[0].columns)
    return pd.concat(dfs, ignore_index=True).iloc[:, 0:ncols]


def get_column_frame(df: pd.DataFrame):
    column_frame = pd.DataFrame(columns=df.columns, index=None, data=None)
    column_frame = column_frame.astype(df.dtypes)
    return column_frame


# Stores a reference to a dataframe that is currently scoped,
# Should be a child of a DataFrameContainerMixinIO
class FrameABC(NodeMixin, ABC):
    def __init__(
        self,
        name,
        parent: ContainerWriterABC,
        if_exists="fail",
        mode: Literal["s", "r", "a", "w", "m"] = "s",
        df: pd.DataFrame = pd.DataFrame(),
        kw_for_pull: Optional[dict] = None,
        # (s)oftread: only loads the name
        # (m)edium read: sample/meta read reads the first rows_for_sampling
        # (r)ead    : nothing yet to be written
        # (a)ppend  : append df to df_target
        # (w)rite   : overwrite df_target with df
    ):
        # df target is where results will be written/appended to on self.write()
        self.df_target: pd.DataFrame = df
        # df is where intermediate operations (truncate, append, etc) are performed
        self.df: pd.DataFrame = df
        self.parent: ContainerWriterABC = parent
        self.mode: Literal["s", "r", "a", "w", "m"] = mode
        self.if_exists: str = if_exists
        if kw_for_pull is None:
            self.kw_for_pull = {}
        else:
            self.kw_for_pull = kw_for_pull

        # If an orphan, name could be optional
        self.name = name

    def read(
        self,
        kwargs=None,
        sample: bool = False,
    ) -> pd.DataFrame:
        if kwargs is None:
            kwargs = {}
        if sample:
            kwargs["nrows"] = nrows_for_sampling
        if self.mode in ("s"):
            self._read(kwargs)
            # when len of df != nrows: sample is assumed to be ignored or small dataset
            if not sample or (sample and len(self.df) != nrows_for_sampling):
                self.mode = "r"
            else:
                self.mode = "m"
        elif self.mode == "m" and not sample:
            self._read(kwargs)
            self.mode = "r"
        return self.df

    def write(self):
        if self.mode not in ("a", "w"):
            return None

        if self.mode == "a" and not self.df_target.empty:
            self.df_target = append_into([self.df_target, self.df])
        else:
            self.df_target = self.df

    @property
    def column_frame(self):
        return get_column_frame(self.df)

    @property
    def append_method(
        self,
    ) -> Literal[
        "frame",
        "file",
    ]:
        return "file"

    def _append(self, df, truncate_first=False):
        if truncate_first and self.append_method == "file":
            self.df = append_into([self.column_frame, df])
        else:
            self.df = append_into([self.df, df])

    def _build(self, df):
        if self.append_method == "frame":
            self.read()
        df = get_column_frame(df)
        self.df_target = df
        self.df = df
        return df

    def set_df(
        self,
        df,
        if_exists,
        kw_for_push=None,
        build=False,
    ):
        self.if_exists = if_exists
        self.kw_for_push = kw_for_push
        # build always builds from the source, does not check against target
        # consistency check done separately
        if build:
            df = self._build(df)
        if self.mode not in ("a", "w"):  # if in read mode, code below is first write
            if if_exists == "fail":
                raise Exception(
                    f"Failing: dataframe {self.name} already exists with mode {self.mode}"
                )
            elif if_exists == "append":
                # ensures alignment of columns with target
                # TODO: might be better to subclass df and have df.truncate.append() ?
                # df = self._build(df)
                self._append(df, truncate_first=True)

                # this dataframe contains only the appended rows
                # thus avoiding rewriting existing data of df
                self.mode = "a"
            elif if_exists == "truncate":
                self._append(df, truncate_first=True)
                self.mode = "w"
            elif if_exists == "replace":
                # df = self._build(df)
                self.df = df
                self.mode = "w"
            else:
                raise Exception(f"if_exists value {if_exists} not supported")
        else:  # if already written once, subsequent calls are appends
            self._append(df)

    @property
    def parent(self) -> ContainerWriterABC:
        return NodeMixin.parent.fget(self)

    @parent.setter
    def parent(self, v):
        NodeMixin.parent.fset(self, v)

    @abstractmethod
    def _read(self, kwargs: dict):
        pass


class ContainerReaderABC(NodeMixin, ABC):
    def __init__(
        self,
        child_class: FrameABC,
        url: str,
        # replace: bool,
    ):
        self.child_class = child_class
        self.url = url
        # self.replace = replace

        # if not self.create_or_replace:
        self._children_init()

    def __contains__(self, child_name):
        for c in self:
            if c.name == child_name:
                return True
        return False

    def __getitem__(self, child_name) -> FrameABC:
        for c in self:
            if c.name == child_name:
                return c
        raise Exception(f"{child_name} not found in {[n.name for n in self]}")

    def __iter__(self) -> Generator[FrameABC, None, None]:
        for child in super().children:
            yield child

    @property
    def mode(self) -> Literal["r", "a", "w"]:
        return "r"

    @property
    def child_names(self) -> list[str]:
        return [child.name for child in self]

    @abstractmethod
    def _children_init(self):
        pass

    @abstractmethod
    def close(self):
        pass
        # perform closing operations on container (file, connection, etc)


class ContainerWriterABC(NodeMixin, ABC):
    def __init__(
        self,
        child_class: FrameABC,
        url: str,
        replace: bool,
    ):
        self.child_class = child_class
        self.url = url
        self.replace = replace

        if not self.create_or_replace:
            self._children_init()

    def __contains__(self, child_name):
        for c in self:
            if c.name == child_name:
                return True
        return False

    def __getitem__(self, child_name) -> FrameABC:
        for c in self:
            if c.name == child_name:
                return c
        raise Exception(f"{child_name} not found in {[n.name for n in self]}")

    def __iter__(self) -> Generator[FrameABC, None, None]:
        for child in super().children:
            yield child

    def fetch_child(
        self,
        df_name,
        df,
        build=False,
    ):
        if build:
            df = get_column_frame(df)
        if df_name not in self:
            self.add_child(
                self.child_class(
                    df=df,
                    name=df_name,
                    parent=self,
                    # fetched+added children are always for writing
                    mode="w",
                )
            )

        return self[df_name]

    @property
    def any_empty_frames(self):
        for df_io in self:
            if df_io.mode in ("a", "w"):
                if df_io.df.empty:
                    return True
        return False

    def write(self):
        # write to target dataframe and then persist to data store
        if self.mode != "r":
            if self.any_empty_frames:
                raise Exception("Cannot write empty dataframe")
            for df_io in self:
                df_io.write()
            self.persist()

    def add_child(self, child: FrameABC):
        child.parent = self

    @property
    def create_or_replace(self):
        return self.replace

    @property
    def mode(self) -> Literal["r", "a", "w"]:
        if self.create_or_replace:
            return "w"
        else:
            for c in self:
                if c.mode in ("a", "w"):
                    return "a"
        return "r"

    @property
    def child_names(self) -> list[str]:
        return [child.name for child in self]

    @abstractmethod
    def _children_init(self):
        pass

    @abstractmethod
    def persist(self):
        pass
        # persist dataframes to data store

    @abstractmethod
    def close(self):
        pass
        # perform closing operations on container (file, connection, etc)
