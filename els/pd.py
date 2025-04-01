import pandas as pd
from anytree import NodeMixin


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
    # df_refs: dict[int, pd.DataFrame] = {}
    # __slots__ = "df"

    def __init__(
        self,
        df: pd.DataFrame,
        name,
        parent: "DataFrameContainerMixinIO",
        mode="r",
    ):
        # self.df_ref = df
        # self.df_random = df
        self.df = df
        self.df_random = df
        self.parent = parent
        # self.df_id = self.fetch_df_id(df)
        self.mode = mode

        # If an orphan, name could be optional
        self.name = name

    # def fetch_df_id(self, df):
    #     if df is None:
    #         raise Exception("Cannot fetch None df")
    #     else:
    #         df_id = id(df)
    #         self.df_refs[df_id] = df
    #         return df_id

    # dataframe for random access
    # different from the df which is the original df reference
    @property
    def df_ref(self) -> pd.DataFrame:
        # return self.df_random
        return self.df_random

    @property
    def df_id2(self):
        return id(self.df_random)

    def close(self):
        pass
        # if self.df_id in self.parent.ram_dfs:
        #     del self.parent.ram_dfs[self.df_id]

    def write(self):
        # if self.mode == "a" and not self.df_random.empty:
        #     df = append_into([self.df_random, self.df])
        # else:
        #     self.parent.ram_dfs[self.df_id] = self.df
        if self.mode == "a" and not self.df_ref.empty:
            # self.df_refs[self.df_id] = append_into([self.df_ref, self.df])
            self.df_random = append_into([self.df_ref, self.df])
        else:
            self.df_random = self.df

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
        df_name,
        df,
        if_exists,
        # build=False,
    ):
        if self.mode == "r":  # if in read mode, code below is first write
            if if_exists == "fail":
                raise Exception(
                    f"Failing: dataframe {df_name} already exists with mode {self.mode}"
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
    replace: str

    def set_df(self, df_id, df):
        raise Exception("set_df must be set in derived classes")
        # self.open_dfs[df_id] = df

    def get_child(self, child_name):
        for c in self.childrens:
            if c.name == child_name:
                return c
        raise Exception(f"{child_name} not found")

    def has_child(self, child_name):
        for c in self.childrens:
            if c.name == child_name:
                return True
        return False

    @property
    def any_empty_frames(self):
        for df_io in self.childrens:
            if df_io.mode not in "r":
                if df_io.df.empty:
                    print(f"cannot write empty dataframe; {df_io.name}: {df_io.df}")
                    return True
        return False

    @property
    def childrens(self) -> tuple[DataFrameIO]:
        return super().children

    def persist():
        # persist dataframes to data store
        raise Exception("persist must be set in derived classes")

    def write(self):
        # write to target dataframe and then persist to data store
        if self.mode != "r":
            if self.any_empty_frames:
                raise Exception("Cannot write empty dataframe")
            for df_io in self.childrens:
                df_io.write()
            self.persist()

    def close(self):
        for df_io in self.childrens:
            df_io.close()

    def add_child(self, child: DataFrameIO):
        child.parent = self

    @property
    def create_or_replace(self):
        return self.replace

    @property
    def mode(self):
        if self.create_or_replace:
            return "w"
        elif len(self.children) == 0:
            return "r"
        else:
            for c in self.childrens:
                if c.mode in ("a", "w"):
                    return "a"
        return "r"

    def fetch_df_io(self, df_name, df):
        if not self.has_child(df_name):
            self.add_child(
                self.child_class(
                    df=df,
                    name=df_name,
                    parent=self,
                    mode="w",
                )
            )
        return self.get_child(df_name)

    @property
    def child_names(self):
        return [child.name for child in self.childrens]


class DataFrameDictIO(DataFrameContainerMixinIO):
    def __init__(
        self,
        df_dict: dict[str, pd.DataFrame],
        replace=False,
    ):
        self.child_class = DataFrameIO
        self._df_dict = df_dict
        self.replace = replace
        if self.mode == "w":
            self.children = []
        else:
            self._children_init()

    def __repr__(self):
        return f"DataFrameDictIO({(self.df_dict, self.replace)})"

    @property
    def df_dict(self):
        for c in self.childrens:
            self._df_dict[c.name] = c.df_ref
            # self.open_dfs[c.df_id]
        return self._df_dict

    @df_dict.setter
    def df_dict(self, v):
        self._df_dict = v

    def _children_init(self) -> dict:
        return [
            DataFrameIO(
                df=df,
                name=name,
                parent=self,
            )
            for name, df in self._df_dict.items()
        ]

    def persist(self):
        for df_io in self.childrens:
            self.df_dict[df_io.name] = df_io.df_ref

    def set_df(
        self,
        df_name,
        df,
        if_exists,
        build=False,
    ):
        if build:
            df = get_column_frame(df)
        df_io = self.fetch_df_io(df_name, df)
        df_io.set_df(
            df_name,
            df,
            if_exists,
        )
