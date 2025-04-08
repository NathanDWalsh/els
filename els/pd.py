from typing import Literal

import pandas as pd
from anytree import NodeMixin

nrows_for_sampling: int = 100


def get_column_frame(df: pd.DataFrame):
    column_frame = pd.DataFrame(columns=df.columns, index=None, data=None)
    column_frame = column_frame.astype(df.dtypes)
    return column_frame


def append_into(dfs: list[pd.DataFrame]):
    # appends subsequent dfs into the first df, keeping only the columns from the first
    ncols = len(dfs[0].columns)
    return pd.concat(dfs, ignore_index=True).iloc[:, 0:ncols]


def multiindex_to_singleindex(df: pd.DataFrame, separator="_"):
    df.columns = [separator.join(map(str, col)).strip() for col in df.columns.values]
    return df


# Stores a reference to a dataframe that is currently scoped,
# Should be a child of a DataFrameContainerMixinIO
class DataFrameIO(NodeMixin):
    def __init__(
        self,
        name,
        parent: "DataFrameContainerMixinIO",
        if_exists="fail",
        mode: Literal["s", "r", "a", "w", "m"] = "s",
        df: pd.DataFrame = pd.DataFrame(),
        # (s)oftread: only loads the name
        # (m)edium read: sample/meta read reads the first rows_for_sampling
        # TODO: consider meta-read where data types are read/infered/data is sampled
        # (r)ead    : nothing yet to be written
        # (a)ppend  : append df to df_target
        # (w)rite   : overwrite df_target with df
    ):
        # df target is where results will be written/appended to on self.write()
        self.df_target: pd.DataFrame = df
        # df is where intermediate operations (truncate, append, etc) are performed
        self.df = df
        self.parent = parent
        self.mode: Literal["s", "r", "a", "w", "m"] = mode
        self.if_exists = if_exists

        # If an orphan, name could be optional
        self.name = name

    def read(self, kwargs={}, sample: bool = False) -> pd.DataFrame:
        if sample:
            kwargs["nrows"] = nrows_for_sampling

        if self.mode in ("s"):
            self._read(kwargs, sample=sample)
            # when len of df != nrows: sample is assumed to be ignored or small dataset
            if not sample or (sample and len(self.df) != nrows_for_sampling):
                self.mode = "r"
            else:
                self.mode = "m"
        elif self.mode == "m" and not sample:
            self._read(kwargs, sample=sample)
            self.mode = "r"
        return self.df

    def _read(self, kwargs={}, sample=False):
        self.df = self.parent.df_dict[self.name]
        self.df_target = self.parent.df_dict[self.name]

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

    def append(self, df, truncate_first=False):
        if truncate_first:
            self.df = append_into([self.column_frame, df])
        else:
            self.df = append_into([self.df, df])

    def set_df(
        self,
        df,
        if_exists,
        kw_for_push=None,
        build=False,
    ):
        self.if_exists = if_exists
        self.kw_for_push = kw_for_push
        if build:
            # TODO: not efficient to load a dataframe and then just take its columns
            # implement idea around meta-read
            df = get_column_frame(df)
            self.df_target = df
            self.df = df
        if self.mode not in ("a", "w"):  # if in read mode, code below is first write
            if if_exists == "fail":
                raise Exception(
                    f"Failing: dataframe {self.name} already exists with mode {self.mode}"
                )
            elif if_exists == "append":
                # ensures alignment of columns with target
                # TODO: might be better to subclass df and have df.truncate.append() ?
                self.append(df, truncate_first=True)

                # this dataframe contains only the appended rows
                # thus avoiding rewriting existing data of df
                self.mode = "a"
            elif if_exists == "truncate":
                self.append(df, truncate_first=True)
                self.mode = "w"
            elif if_exists == "replace":
                self.df = df
                self.mode = "w"
            else:
                raise Exception(f"if_exists value {if_exists} not supported")
        else:  # if already written once, subsequent calls are appends
            self.append(df)

    @property
    def parent(self) -> "DataFrameContainerMixinIO":
        return NodeMixin.parent.fget(self)

    @parent.setter
    def parent(self, v):
        NodeMixin.parent.fset(self, v)


class DataFrameContainerMixinIO(NodeMixin):
    child_class: DataFrameIO
    replace: bool
    df_dict: dict[str, pd.DataFrame]

    def get_child(self, child_name):
        for c in self.childrens:
            if c.name == child_name:
                return c
        raise Exception(f"{child_name} not found in {[n.name for n in self.childrens]}")

    def has_child(self, child_name):
        for c in self.childrens:
            if c.name == child_name:
                return True
        return False

    def fetch_child(
        self,
        df_name,
        df,
        build=False,
    ):
        if build:
            df = get_column_frame(df)
        if not self.has_child(df_name):
            self.add_child(
                self.child_class(
                    df=df,
                    name=df_name,
                    parent=self,
                    # fetched+added children are always for writing
                    mode="w",
                    # if_exists="",
                )
            )

        return self.get_child(df_name)

    @property
    def any_empty_frames(self):
        for df_io in self.childrens:
            if df_io.mode in ("a", "w"):
                if df_io.df.empty:
                    print(f"empty dataframe found: {df_io.name}: {df_io.df}")
                    return True
        return False

    @property
    def childrens(self) -> tuple[DataFrameIO]:
        return super().children

    def write(self):
        # write to target dataframe and then persist to data store
        if self.mode != "r":
            if self.any_empty_frames:
                raise Exception("Cannot write empty dataframe")
            for df_io in self.childrens:
                df_io.write()
            self.persist()

    def add_child(self, child: DataFrameIO):
        child.parent = self

    @property
    def create_or_replace(self):
        return self.replace

    @property
    def mode(self) -> Literal["r", "a", "w"]:
        if self.create_or_replace:
            return "w"
        else:
            for c in self.childrens:
                if c.mode in ("a", "w"):
                    return "a"
        return "r"

    @property
    def child_names(self):
        # TODO: better to change to tuple?
        return [child.name for child in self.childrens]

    def _children_init():
        raise Exception("_children_init must be set in derived classes")

    def persist():
        # persist dataframes to data store
        raise Exception("persist must be set in derived class")

    def close():
        # perform closing operations on container (file, connection, etc)
        raise Exception("close must be set in derived class")


class DataFrameDictIO(DataFrameContainerMixinIO):
    def __init__(
        self,
        df_dict: dict[str, pd.DataFrame],
        replace=False,
    ):
        self.child_class = DataFrameIO
        self.df_dict = df_dict
        self.replace = replace
        if not self.create_or_replace:
            self._children_init()

    def __repr__(self):
        return f"DataFrameDictIO({(self.df_dict, self.replace)})"

    def _children_init(self) -> dict:
        for name in self.df_dict.keys():
            DataFrameIO(
                name=name,
                parent=self,
            )

    def persist(self):
        for df_io in self.childrens:
            if df_io.mode in ("a", "w"):
                self.df_dict[df_io.name] = df_io.df_target

    def close():
        pass
        # no closing operations required for dataframe
