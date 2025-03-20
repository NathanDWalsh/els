from functools import cached_property

from anytree import NodeMixin

import els.core as el


class DataFrameIO(NodeMixin):
    # def __init__(self, df, parent, name, mode="r"):
    def __init__(self, df, name, mode="r"):
        self.df = df
        self.df_id = el.fetch_df_id(df)
        self.mode = mode

        # self.parent = parent
        self.name = name

    @property
    def open_df(self):
        return el.open_dfs[self.df_id]

    @cached_property
    def column_frame(self):
        return el.get_column_frame(self.df)

    def write(self):
        if self.mode == "a" and not self.open_df.empty:
            el.open_dfs[self.df_id] = el.append_into([self.open_df, self.df])
        else:
            el.open_dfs[self.df_id] = self.df

    def append(self, df, truncate_first=False):
        if truncate_first:
            self.df = el.append_into([self.column_frame, df])
        else:
            self.df = el.append_into([self.df, df])

    def set_df(self, df_name, df, if_exists):
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

    def close(self):
        if self.df_id in el.open_dfs:
            del el.open_dfs[self.df_id]


class DataFrameContainerMixinIO(NodeMixin):
    def get_child(self, child_name):
        for c in self.children:
            if c.name == child_name:
                return c
        return None

    def has_child(self, child_name):
        for c in self.children:
            if c.name == child_name:
                return True
        return False

    @property
    def any_empty_frames(self):
        for df_io in self.children:
            if df_io.mode not in "r":
                if df_io.df.empty:
                    print(f"cannot write empty dataframe; {df_io.name}: {df_io.df}")
                    return True
        return False

    def write(self):
        if self.mode != "r":
            if self.any_empty_frames:
                raise Exception("Cannot write empty dataframe")
            for df_io in self.children:
                df_io.write()
            self.persist()

    def add_child(self, child):
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
            for c in self.children:
                if c.mode in ("a", "w"):
                    return "a"
        return "r"

    def fetch_df_io(self, df_name, df):
        if not self.has_child(df_name):
            self.add_child(self.child_class(df=df, name=df_name, mode="w"))
        return self.get_child(df_name)

    @property
    def child_names(self):
        return [child.name for child in self.children]


class DataFrameDictIO(DataFrameContainerMixinIO):
    def __init__(self, df_dict, replace=False):
        self.child_class = DataFrameIO
        self._df_dict = df_dict
        self.replace = replace
        self.children = [] if self.mode == "w" else self._children_init()

    @property
    def df_dict(self):
        for c in self.children:
            self._df_dict[c.name] = c.open_df
        return self._df_dict

    @df_dict.setter
    def df_dict(self, v):
        self._df_dict = v

    def _children_init(self) -> dict:
        return [
            DataFrameIO(
                df=df,
                name=name,
            )
            for name, df in self._df_dict.items()
        ]

    def persist(self):
        for df_io in self.children:
            self.df_dict[df_io.name] = df_io.open_df

    def set_df(self, df_name, df, if_exists):
        df_io = self.fetch_df_io(df_name, df)
        df_io.set_df(df_name, df, if_exists)
