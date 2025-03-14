from functools import cached_property

from anytree import NodeMixin

import els.core as el


class DataFrameIO(NodeMixin):
    def __init__(self, df, parent, name, mode="r"):
        self.df = df
        self.df_id = el.fetch_df_id(df)
        self.mode = mode

        self.parent = parent
        self.name = name

    @property
    def open_df(self):
        return el.open_dfs[self.df_id]

    @cached_property
    def column_frame(self):
        return el.get_column_frame(self.df)

    def write(self):
        if self.mode == "a":
            el.open_dfs[self.df_id] = el.append_into([self.open_df, self.df])
        elif self.mode == "w":
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
            else:
                raise Exception(f"if_exists value {if_exists} not supported")
        else:  # if already written once, subsequent calls are appends
            self.append(df)


class DataFrameDictIO(NodeMixin):
    def __init__(self, df_dict, replace=False):
        self._df_dict = df_dict
        self.replace = replace
        self.children = self._get_df_ios()

    @property
    def df_dict(self):
        for c in self.children:
            self._df_dict[c.name] = c.open_df
        return self._df_dict

    @df_dict.setter
    def df_dict(self, v):
        self._df_dict = v

    def _get_df_ios(self) -> dict:
        if self.replace:
            return []
        else:
            res = [
                DataFrameIO(df=v, parent=self, name=k) for k, v in self._df_dict.items()
            ]
            return res

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
                self.df_dict[df_io.name] = df_io.open_df

    def add_child(self, child):
        child.parent = self

    def fetch_df_io(self, df_name, df) -> DataFrameIO:
        if not self.has_child(df_name):
            self.add_child(DataFrameIO(df=df, parent=self, name=df_name, mode="w"))
        return self.get_child(df_name)

    def set_df(self, df_name, df, if_exists):
        df_io = self.fetch_df_io(df_name, df)
        df_io.set_df(df_name, df, if_exists)

    @cached_property
    def mode(self):
        if len(self.children) == 0:
            return "r"
        elif self.replace:
            return "w"
        else:
            for io in self.children:
                if io.mode in ("a", "w"):
                    return "a"
        return "r"
