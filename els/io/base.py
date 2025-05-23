from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Generic, Optional, Protocol, TypeVar

import pandas as pd

if TYPE_CHECKING:
    from collections.abc import Generator, Sequence

    from els._typing import (
        ContainerModeLiteral,
        FrameModeLiteral,
        IfExistsLiteral,
        KWArgsIO,
    )


nrows_for_sampling: int = 100


def multiindex_to_singleindex(
    df: pd.DataFrame,
    separator: str = "_",
) -> pd.DataFrame:
    df.columns = [
        separator.join(map(str, col)).strip() for col in df.columns.to_numpy()
    ]
    return df


def append_into(dfs: Sequence[pd.DataFrame]) -> pd.DataFrame:
    dfs = list(dfs)
    # appends subsequent dfs into the first df, keeping only the columns from the first
    ncols = len(dfs[0].columns)
    return pd.concat(dfs, ignore_index=True).iloc[:, 0:ncols]


def get_column_frame(df: pd.DataFrame) -> pd.DataFrame:
    column_frame = pd.DataFrame(columns=df.columns, index=None, data=None)
    column_frame = column_frame.astype(df.dtypes)
    return column_frame


TFrame = TypeVar("TFrame", bound="FrameABC")


# Stores a reference to a dataframe that is currently scoped,
# Should be a child of a DataFrameContainerMixinIO
class FrameABC(ABC):
    def __init__(
        self,
        name: str,
        parent: ContainerReaderABC,
        if_exists: IfExistsLiteral = "fail",
        mode: FrameModeLiteral = "s",
        df: pd.DataFrame = pd.DataFrame(),
        kwargs_pull: Optional[KWArgsIO] = None,
    ) -> None:
        self.name = name
        self.parent = parent
        self.if_exists = if_exists
        self.mode = mode
        # where results will be written/appended to on self.write():
        self.df_target = df
        # where intermediate operations (truncate, append, etc) are performed:
        self.df = df
        self.kwargs_pull = kwargs_pull or {}

    def read(
        self,
        kwargs: Optional[KWArgsIO] = None,
        sample: bool = False,
    ) -> pd.DataFrame:
        kwargs = kwargs or {}
        if sample:
            kwargs["nrows"] = nrows_for_sampling
        if self.mode in ("s"):
            self._read(kwargs)
            if (
                not sample
                # when len(df) > nrows: sample was ignored due to kwargs
                # when len(df) < rorws: small dataset
                or (sample and len(self.df) != nrows_for_sampling)
            ):
                self.mode = "r"
            else:
                self.mode = "m"
        elif self.mode == "m" and not sample:
            self._read(kwargs)
            self.mode = "r"
        return self.df

    def write(self) -> None:
        if self.mode not in ("a", "w"):
            return
        elif self.mode == "a" and not self.df_target.empty:
            self.df_target = append_into([self.df_target, self.df])
        else:
            self.df_target = self.df

    @property
    def column_frame(self) -> pd.DataFrame:
        return get_column_frame(self.df)

    def append(
        self,
        df: pd.DataFrame,
        truncate_first: bool = False,
    ) -> None:
        if truncate_first:
            self.df = append_into([self.column_frame, df])
        else:
            self.df = append_into([self.df, df])

    def build(self, df: pd.DataFrame) -> pd.DataFrame:
        df = get_column_frame(df)
        self.df_target = df
        self.df = df
        return df

    def set_df(
        self,
        df: pd.DataFrame,
        if_exists: IfExistsLiteral,
        kwargs_push: Optional[KWArgsIO] = None,
        build: bool = False,
    ) -> None:
        self.if_exists = if_exists
        self.kwargs_push = kwargs_push or {}
        # build always builds from the source, does not check against target
        # consistency check done separately
        if build:
            df = self.build(df)
        if self.mode not in ("a", "w"):  # if in read mode, code below is first write
            if if_exists == "fail":
                raise Exception(
                    f"Failing: dataframe {self.name} already exists with mode {self.mode}"
                )
            elif if_exists == "append":
                # ensures alignment of columns with target
                self.append(df, truncate_first=True)

                # this dataframe contains only the appended rows
                # thus avoiding rewriting existing data of df
                self.mode = "a"
            elif if_exists == "truncate":
                self.append(df, truncate_first=True)
                self.mode = "w"
            elif if_exists == "replace":
                # df = self._build(df)
                self.df = df
                self.mode = "w"
            else:
                raise Exception(f"if_exists value {if_exists} not supported")
        else:  # if already written once, subsequent calls are appends
            self.append(df)

    @abstractmethod
    def _read(self, kwargs: KWArgsIO) -> None:
        pass


class ContainerProtocol(Protocol):
    def __init__(self, url: str, replace: bool) -> None: ...
    def close(self) -> None: ...
    @property
    def child_names(self) -> list[str]: ...

    # def fetch_child(
    #     self,
    #     df_name: str,
    #     df: pd.DataFrame,
    #     build=False,
    # ) -> TFrame:
    # ...


class ContainerReaderABC(ABC, Generic[TFrame]):
    # class ContainerReaderABC(ABC):
    def __init__(
        self,
        child_class: type[TFrame],
        url: str,
        # replace: bool,
    ) -> None:
        self.children: list[TFrame] = []
        self.child_class = child_class
        self.url = url
        self._children_init()

    def __contains__(self, child_name: str) -> bool:
        for c in self:
            if c.name == child_name:
                return True
        return False

    def __getitem__(self, child_name: str) -> TFrame:
        for c in self:
            if c.name == child_name:
                return c
        raise Exception(f"{child_name} not found in {[n.name for n in self]}")

    def __iter__(self) -> Generator[TFrame, None, None]:
        for child in self.children:
            yield child

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({(self.url)})"

    @property
    def mode(self) -> ContainerModeLiteral:
        return "r"

    @property
    def child_names(self) -> list[str]:
        return [child.name for child in self]

    @abstractmethod
    def _children_init(self) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass
        # perform closing operations on container (file, connection, etc)


class ContainerWriterABC(ContainerReaderABC[TFrame], Generic[TFrame]):
    # class ContainerWriterABC(ContainerReaderABC):
    def __init__(
        self,
        child_class: type[TFrame],
        url: str,
        replace: bool,
    ):
        self.child_class = child_class
        self.url = url
        self.replace = replace
        self.children: list[TFrame] = []

        if not self.create_or_replace:
            self._children_init()

    def fetch_child(
        self: ContainerReaderABC,
        df_name: str,
        df: pd.DataFrame,
        build: bool = False,
    ) -> TFrame:
        if build:
            df = get_column_frame(df)
        if df_name not in self:
            self.children.append(
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
    def any_empty_frames(self) -> bool:
        for df_io in self:
            if df_io.mode in ("a", "w"):
                if df_io.df.empty:
                    return True
        return False

    def write(self) -> None:
        # write to target dataframe and then persist to data store
        if self.mode != "r":
            if self.any_empty_frames:
                raise Exception("Cannot write empty dataframe")
            for df_io in self:
                df_io.write()
            self.persist()

    # def add_child(self: TContainer, child: TFrame) -> None:
    #     child.parent = self

    @property
    def create_or_replace(self) -> bool:
        return self.replace

    @property
    def mode(self) -> ContainerModeLiteral:
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
    def persist(self) -> None:
        pass
        # persist dataframes to data store
