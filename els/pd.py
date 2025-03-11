from functools import cached_property

import pandas as pd

import els.core as el


class DataFrameDictIO:
    def __init__(self, df_dict, replace=False):
        self.df_dict = df_dict
        self.replace = replace

        # self.dfdict_io = el.fetch_dfdict_io(df_dict, replace=self.replace)
        self.dfs = self.get_df_deets()
        # raise Exception(self.dfs)

    @cached_property
    def create_or_replace(self):
        # if self.replace or not self.url:
        if self.replace:
            return True
        else:
            return False

    def get_df_deets(self) -> dict:
        if self.create_or_replace:
            return {}
        else:
            res = {k: {"mode": "r", "df": v} for k, v in self.df_dict.items()}
            # res = {k: {"mode": "r"} for k, v in self.df_dict.items()}
            # raise Exception(res)
            return res

    def pull_df_structure(self, df_name):
        df = self.dfs[df_name]["df"]
        return el.get_column_frame(df)

    def write(self):
        if self.mode != "r":
            print("write")
            for df_name, df_deet in self.dfs.items():
                if df_deet["mode"] not in "r":
                    df = df_deet["df"]
                    if df.empty:
                        raise Exception(
                            f"cannot write empty dataframe; {df_name}: {df}"
                        )
            if self.mode == "w":
                print("writing")
                for df_name, df_deet in self.dfs.items():
                    df = df_deet["df"]
                    self.df_dict[df_name] = df
            else:  # self.mode == "a"
                for df_name, df_deet in self.dfs.items():
                    if df_deet["mode"] != "r":
                        df = df_deet["df"]
                        if df_deet["mode"] == "a":
                            df0 = self.df_dict[df_name]
                            print(f"appending {df} to {df0}")
                            self.df_dict[df_name] = pd.concat(
                                [df0, df], ignore_index=True
                            )
                            print(f"write dict {id(self.df_dict)}")
                            print(f"res: {self.df_dict[df_name]}")
                        else:
                            self.df_dict[df_name] = df

    def set_new_df_df(self, sheet_name, df):
        self.dfs[sheet_name] = {
            "mode": "w",
            "if_exists": "replace",  # not sure about this
            "df": df,
        }

    def set_df_df(self, df_name, df, if_exists):
        print(f"set_df_df: {[self.dfs, if_exists]}")
        if df_name in self.dfs:
            if self.dfs[df_name]["mode"] == "r":
                if not isinstance(if_exists, str):
                    if_exists = if_exists.value
                if if_exists == "fail":
                    raise Exception(
                        f"Failing: dataframe {df_name} already exists with mode {self.dfs[df_name]['mode']}"
                    )
                elif if_exists == "replace":
                    self.set_new_df_df(df_name, df)
                elif if_exists == "append":
                    # ensures alignment of columns with target
                    df0 = self.pull_df_structure(df_name)
                    df = el.append_into([df0, df])

                    # this dataframe contains only the appended rows
                    # thus avoiding rewriting existing data of df
                    self.dfs[df_name]["mode"] = "a"
                elif if_exists == "truncate":
                    df0 = self.pull_df_structure(df_name)
                    df = el.append_into([df0, df])
                    # raise Exception([df0, df, df_name])
                    self.dfs[df_name]["mode"] = "w"
                else:
                    raise Exception(f"if_exists value {if_exists} not supported")
            else:
                df0 = self.dfs[df_name]["df"]
                df = el.append_into([df0, df])
                # df = pd.concat([df0, df], ignore_index=True)
        else:
            self.set_new_df_df(df_name, df)
        self.dfs[df_name]["df"] = df

    def close(self):
        pass  # TODO:TEST; will it help with memory/garbage?
        # del el.open_dicts[self.url]

    @cached_property
    def mode(self):
        if len(self.dfs) == 0:
            return "r"
        elif self.create_or_replace:
            return "w"
        else:
            for deet in self.dfs.values():
                if deet["mode"] in ("a", "w"):
                    return "a"
        return "r"
