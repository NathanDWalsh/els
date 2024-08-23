from pathlib import Path

# import zipfile
from anytree import NodeMixin, RenderTree, PreOrderIter
import pandas as pd
import os
from stat import FILE_ATTRIBUTE_HIDDEN
from typing import Union, Callable, Optional, TypeAlias, Self

# from collections.abc import Generator
from enum import Enum

# from openpyxl import load_workbook
from python_calamine import CalamineWorkbook, SheetTypeEnum, SheetVisibleEnum
import yaml
import logging
import typer

import eel.config as ec
import eel.flow as ef
import eel.execute as ee
from eel.pathprops import HumanPathPropertiesMixin

CONFIG_FILE_EXT = ".eel.yml"
FOLDER_CONFIG_FILE_STEM = "_"
ROOT_CONFIG_FILE_STEM = "__"

config_dict_type: TypeAlias = dict[str, dict[str, str]]


class NodeType(Enum):
    CONFIG_DIRECTORY = "config directory"
    CONFIG_EXPLICIT = "explicit config"
    CONFIG_VIRTUAL = "virtual config"
    # CONFIG_DOC = "config_doc"
    DATA_URL = "source url"
    DATA_TABLE = "data_table"


class FileType(Enum):
    EXCEL = "excel"
    CSV = "csv"
    EEL = "eel"
    FWF = "fixed width file"
    XML = "xml"

    @classmethod
    def suffix_to_type(cls, extension: str):
        mapping = {
            "xlsx": cls.EXCEL,
            "xls": cls.EXCEL,
            "xlsm": cls.EXCEL,
            "xlsb": cls.EXCEL,
            "csv": cls.CSV,
            "tsv": cls.CSV,
            # TODO: handle double extension eel.yml
            # for now assumes any yml file is an eel config
            "yml": cls.EEL,
            "fwf": cls.FWF,
            "xml": cls.XML,
        }
        return mapping.get(extension.lower().strip("."), None)


def get_folder_config_name():
    return FOLDER_CONFIG_FILE_STEM + CONFIG_FILE_EXT


def get_root_config_name():
    return ROOT_CONFIG_FILE_STEM + CONFIG_FILE_EXT


class ContentAwarePath(Path, HumanPathPropertiesMixin, NodeMixin):
    _flavour = type(Path())._flavour  # type: ignore

    def __init__(
        self,
        *args,
        parent: Optional[Self] = None,
        spawn_children: Optional[bool] = False,
        config: dict = None,
        **kwargs,
    ):
        # if self.is_file() and not self.is_config_file()

        self.parent = parent

        if self.is_dir() or self.is_config_file():
            if config:
                raise Exception(
                    "should not pass explicit config for directories or config files"
                )
            paired_config = self.get_paired_config()

            if self.is_dir():
                self._config = paired_config
            elif self.is_config_file:

                self._config = {}
                # print(f"spawning {self}")
                self.spawn_document_children(paired_config)
                # print(f"fininshed spawning {self}")
        elif not config:
            raise Exception("should pass explicit config for urls and tables")
        else:
            self._config = config

        if spawn_children:
            if self.is_dir():
                self.spawn_config_children()

        if (self.is_dir() and spawn_children and not self.has_leaf_table) or (
            self in self.siblings
        ):
            # do not add dirs with no leaf nodes which are tables
            # TODO this could be changed to search for config files instead ...
            # ... making debugging faulty config files easier
            # pass
            self.parent = None

        # if (
        #     spawn_children
        #     and self.is_file()
        #     and not self.is_config_file()
        #     and len(self.children) == 0
        # ):
        #     # print(self.parent.parent._config.model_dump(exclude_none=True))
        #     # print(self.parent._config)
        #     # print(self._config)
        #     # print(self.config.model_dump(exclude_none=True))
        #     # raise Exception()

        #     tables = get_content_leaf_names(self.config.source)
        #     for table in tables:
        #         ContentAwarePath(
        #             self / table, parent=self, config={"source": {"table": table}}
        #         )
        # raise Exception(self.children)

        # else:
        #     self.parent = parent

        # print(self)
        # print(self.children)

    def spawn_document_children(self, config_docs):
        last_url = ""
        for doc in config_docs:
            # print(doc)
            if "source" in doc:
                source = doc["source"]
            else:
                source = self.parent.config.source.model_dump(exclude_none=True)
                # raise Exception(
                #     f"source not found in one of the documents in config {self}"
                # )
            if "url" in source and last_url != source["url"]:
                last_url = source["url"]
                # print(f"data url: {last_url}")
                # url_parent = ContentAwarePath(self / last_url, parent=self, config=doc)
                url_parent = ContentAwarePath(Path(last_url), parent=self, config=doc)
                # raise Exception("not implemented")

                # print(f"url parent, child: {self}, {url_parent}")
            # print(url_parent.children)
            # print(self.node_type)
            # raise Exception("not implemented")
            if "table" in source:
                source_table = source["table"]
            elif (
                not self.node_type == NodeType.CONFIG_VIRTUAL
                and not self.is_config_adjacent
                and self.parent
                and self.parent.config
                and self.parent.config.source
                and self.parent.config.source.table
            ):
                source_table = self.parent.config.source.table
            else:
                # raise Exception()
                source_table = "_leaf_name"
            if source_table:
                # raise Exception()
                if last_url == "":
                    raise Exception("expected to have a url for child config doc")
                # print(f"table: {source['table']}")
                if source_table == "_leaf_name":
                    source_table = get_content_leaf_names(url_parent.config.source)[0]
                # print(f"self / last_url / table: {self} / {last_url} / {table}")
                # ContentAwarePath(self / last_url / table, parent=url_parent, config=doc)
                ContentAwarePath(
                    Path(last_url) / source_table, parent=url_parent, config=doc
                )

        # print(f"tab parent, child: {url_parent}, {last_table}")

    def spawn_config_children(self):
        for subpath in self.glob("*"):
            # ensure not dir config nor root config
            if subpath.name in (
                get_folder_config_name(),
                get_root_config_name(),
            ):
                pass
            elif config_path_valid(subpath):
                # raise Exception()
                if (
                    not subpath.is_dir() and not subpath.is_config_file()
                ):  # an implicit config
                    ContentAwarePath(
                        str(subpath) + CONFIG_FILE_EXT,
                        parent=self,
                        spawn_children=True,
                    )
                elif subpath not in (
                    self.children
                ):  # a directory or an explicit config file
                    ContentAwarePath(subpath, parent=self, spawn_children=True)

            else:
                logging.warning(f"Invalid path not added to tree: {str(subpath)}")

    @property
    def node_type(self) -> NodeType:
        if self.is_dir():
            return NodeType.CONFIG_DIRECTORY
        elif self.is_config_file():
            if self.is_file():
                return NodeType.CONFIG_EXPLICIT
            else:
                return NodeType.CONFIG_VIRTUAL
        elif self.is_file():
            return NodeType.DATA_URL
        elif self.parent.is_config_file():
            return NodeType.DATA_URL
        else:
            return NodeType.DATA_TABLE

    @property
    def is_config_adjacent(self) -> bool:
        if self.node_type == NodeType.CONFIG_EXPLICIT:
            return Path(str(self).replace(CONFIG_FILE_EXT, "")).exists()
        return False

    @property
    def get_leaf_tables(self) -> list[Self]:
        leaf_tables = []
        for leaf in self.leaves:
            if leaf.node_type == NodeType.DATA_TABLE:
                leaf_tables.append(leaf)
        # print(leaf_tables)
        return leaf_tables

    @property
    def has_leaf_table(self) -> bool:
        return self.get_leaf_tables != []

    @property
    def config_file_path(self) -> Optional[str]:
        if self.node_type == NodeType.CONFIG_DIRECTORY:
            if self.is_root:
                return f"{self.abs}\{get_root_config_name()}"
            else:
                return f"{self.abs}\{get_folder_config_name()}"
        elif (
            self.node_type == NodeType.CONFIG_EXPLICIT
            or self.node_type == NodeType.CONFIG_VIRTUAL
        ):
            return str(self.abs)
        elif self.node_type == NodeType.DATA_URL:
            return str(self.parent.abs)
        elif self.node_type == NodeType.DATA_TABLE:
            return str(self.parent.parent.abs)

    def config_raw(self, add_config_file_path=False) -> ec.Config:
        config_line = []
        # if root eel config is mandatory, this "default dump line" is not required
        config_line.append(ec.Config().model_dump(exclude_none=True))

        for node in self.ancestors + (self,):
            if node._config:
                if isinstance(node._config, ec.Config):
                    config_line.append(node._config.model_dump(exclude_none=True))
                else:
                    config_line.append(node._config)

        config_merged = ContentAwarePath.merge_configs(*config_line)
        config_copied = config_merged.model_copy(deep=True)
        if add_config_file_path:
            config_copied.config_path = self.config_file_path
        # res = {**{"config_path": self.config_file_path}, **config_copied}
        # if str(self) == "world_bank":
        #     raise Exception(config_copied.model_dump(exclude_none=True))

        return config_copied

    @property
    def config(self) -> ec.Config:
        config_copied = self.config_raw()

        # if self.is_leaf:
        config_evaled = self.eval_dynamic_attributes(config_copied)
        # else:
        #     config_evaled = config_copied

        if not config_evaled.target.if_exists:
            config_evaled.target.if_exists = ec.TargetIfExistsValue.FAIL

        return config_evaled

    def get_path_props_find_replace(self) -> dict:
        res = {}
        for member in ec.DynamicPathValue:  # type: ignore
            path_val = getattr(self, member.value[1:])
            res[member.value] = path_val
        return res

    def eval_dynamic_attributes(self, config: ec.Config) -> ec.Config:
        config_dict = config.model_dump(exclude_none=True)
        find_replace = self.get_path_props_find_replace()
        ContentAwarePath.swap_dict_vals(config_dict, find_replace)
        if (
            self.is_leaf
            and config_dict
            and "target" in config_dict
            and "table" in config_dict["target"]
            and "url" in config_dict["target"]
            and "*" in config_dict["target"]["url"]
        ):
            config_dict["target"]["url"] = config_dict["target"]["url"].replace(
                "*", config_dict["target"]["table"]
            )

        res = ec.Config(**config_dict)
        return res

    @staticmethod
    def swap_dict_vals(dictionary: dict, find_replace_dict: dict) -> None:
        for key, value in dictionary.items():
            if isinstance(value, dict):
                ContentAwarePath.swap_dict_vals(dictionary[key], find_replace_dict)
            elif isinstance(value, list):
                pass
            elif value in find_replace_dict:
                dictionary[key] = find_replace_dict[value]
            # elif key == "url" and "*" in value:
            #     dictionary[key] = value.replace("*", find_replace_dict["_leaf_name"])

    @staticmethod
    def merge_configs(*configs: list[Union[ec.Config, config_dict_type]]) -> ec.Config:
        dicts: list[dict] = []
        for config in configs:
            if isinstance(config, ec.Config):
                dicts.append(config.model_dump(exclude={"children"}))
            elif isinstance(config, dict):
                # append all except children
                config_to_append = config.copy()
                if "children" in config_to_append:
                    config_to_append.pop("children")
                dicts.append(config_to_append)
            else:
                raise Exception("configs should be a list of Configs or dicts")
        dict_result = ContentAwarePath.merge_dicts_by_top_level_keys(*dicts)
        res = ec.Config(**dict_result)  # type: ignore
        return res

    @staticmethod
    def merge_dicts_by_top_level_keys(*dicts: config_dict_type) -> config_dict_type:
        merged_dict: dict = {}
        for dict_ in dicts:
            for key, value in dict_.items():
                if (
                    key in merged_dict
                    and isinstance(value, dict)
                    and (not merged_dict[key] is None)
                ):
                    merged_dict[key].update(value)
                elif value is not None:
                    # Add a new key-value pair to the merged dictionary
                    merged_dict[key] = value
        return merged_dict

    def get_paired_configs(self) -> list[dict]:
        res = []
        if self.is_dir():
            if self.is_root:
                config_path = self / get_root_config_name()
                if config_path.exists():
                    ymls = get_yml_docs(config_path, expected=1)
                    res.append(ymls[0])
            config_path = self / get_folder_config_name()
            if config_path.exists():
                ymls = get_yml_docs(config_path, expected=1)
                res.append(ymls[0])
        if self.is_config_file():
            adjacent_file_path = Path(str(self).replace(CONFIG_FILE_EXT, ""))
            if adjacent_file_path.is_file():
                if self.exists():
                    # raise Exception()
                    yml = get_yml_docs(self)

                    if "source" in yml[0] and "url" in yml[0]["source"]:
                        if yml[0]["source"]["url"] == str(adjacent_file_path):
                            res += yml
                        else:
                            raise Exception(
                                f"adjacent config {self} has url: {yml[0]['source']['url']} different than its adjacent data file: {adjacent_file_path}"
                            )
                    elif len(yml) == 1:
                        # raise Exception(yml[0].keys())
                        if "source" in yml[0].keys():
                            yml[0]["source"]["url"] = str(adjacent_file_path)
                            # tables = get_content_leaf_names(
                            #     ec.Source(url=str(adjacent_file_path))
                            # )
                            # if len(tables) == 1:
                            #     yml[0]["source"]["table"] = tables[0]
                        else:
                            yml[0]["source"] = {"url": str(adjacent_file_path)}

                        res += yml

                    else:
                        res.append({"source": {"url": str(adjacent_file_path)}})

                        res += yml
                        # raise Exception(self)
                else:
                    res.append({"source": {"url": str(adjacent_file_path)}})
                    tables = get_content_leaf_names(
                        ec.Source(url=str(adjacent_file_path))
                    )
                    for table in tables:
                        if (
                            not self.parent
                            or not self.parent.config
                            or not self.parent.config.source
                            or not self.parent.config.source.table
                            or table == self.parent.config.source.table
                        ):
                            res.append({"source": {"table": table}})
                        # raise Exception(self.parent.config)

            elif self.exists():
                yml = get_yml_docs(self)
                res += yml
                # raise Exception()

        return res

    def get_paired_config(self) -> dict:
        paired_configs = self.get_paired_configs()
        # return ContentAwarePath.merge_configs(*paired_configs)
        if self.is_dir():
            if len(paired_configs) > 0:
                return ContentAwarePath.merge_configs(*paired_configs)
            else:
                return {}
        return paired_configs

    def get_paired_config_path(self) -> Optional[Path]:
        # order matters, file is checked first in case a single data file passed
        if self.is_config_file():
            return self
        # TODO: add scan root option: now assumes a root config present in passed dir
        elif self.is_root:
            config_path = self / get_root_config_name()
        elif self.is_dir():
            config_path = self / get_folder_config_name()
        else:
            return None
        if config_path.exists():
            return config_path
        return None

    def display_tree(self):
        column1_width = 0
        column2_width = 0
        rows = []
        for pre, fill, node in RenderTree(self):
            column2 = ""
            if node.is_root and node.is_dir():
                column1 = f"{pre}{str(node.abs.name)}"
                # column2 = f": {node.node_type.value}"
                # column1 = f"{pre}{str(node.abs.name)}"
            elif node.node_type == NodeType.DATA_TABLE:
                column1 = f"{pre}{node.name}"
            # TODO: this might be useful
            # elif node.node_type == NodeType.DATA_URL:
            #     column1 = f"{pre}{node.config.source.url}"
            elif (
                node.node_type == NodeType.DATA_URL
                and not Path(node.config.source.url).exists()
            ):
                # column1 = f"{pre}{node.name}"
                url_branch = (
                    str(node.path[-1])
                    .split("?")[0]
                    .replace("\\", "/")
                    .replace(":", ":/")
                )
                column1 = f"{pre}{url_branch}"
                # column2 = f" : {node.node_type.value}"
            else:
                column1 = f"{pre}{node.name}"

            # column2 = ""
            if node.node_type == NodeType.DATA_TABLE and node.config.target.url:
                if node.config.target.type == ".csv":
                    target_path = os.path.relpath(node.config.target.url)
                else:
                    target_path = f"{node.config.target.url.split('?')[0]}#{node.config.target.table}"

                column2 = f" → {target_path}"
            # elif node.is_leaf and node.config.source.url:
            elif node.is_leaf and node.config.target.type == "pandas":
                column2 = f" → memory['{node.config.target.table}']"

            rows.append((column1, column2))

            if column2 != "":  # only count if there is a second column
                column1_width = max(column1_width, len(column1))
                column2_width = max(column2_width, len(column2))

        for column1, column2 in rows:
            # if column2 == "":
            #     typer.echo(column1)
            # else:
            typer.echo(f"{column1:{column1_width}}{column2}".rstrip())

    @property
    def parent(self):
        # return NodeMixin().parent
        if NodeMixin.parent.fget is not None:
            return NodeMixin.parent.fget(self)
        else:
            return self

    @parent.setter
    def parent(self, value):
        if NodeMixin.parent.fset:
            NodeMixin.parent.fset(self, value)

    @property
    def root(self):
        if NodeMixin.root.fget:
            return NodeMixin.root.fget(self)
        else:
            return self

    # @property
    def is_data_table(self) -> bool:
        # """
        # Check if the path points to a content inside a file.
        # A naive check is to see if the parent exists as a file.
        # """
        # if self.is_root or not self.parent:
        #     return False
        # else:
        #     return self.parent.is_file()
        return self.node_type == NodeType.DATA_TABLE

    def is_config_file(self) -> bool:
        return str(self).endswith(CONFIG_FILE_EXT)

    @property
    def subdir_patterns(self) -> list[str]:
        # TODO patterns may overlap
        children = (
            self._config["children"]
            if self._config and "children" in self._config
            else None
        )
        if children is None or children == {}:
            res = ["*"]
        elif isinstance(children, str):
            res = [str(children)]  # recasting as str for linter
        elif isinstance(children, dict):
            # get key of each dict as list entries
            res = list(children.keys())
        elif isinstance(children, list):
            res = children
        # if list of dicts
        else:
            raise Exception("Unexpected children")
        return res

    def get_url_leaf_names(self) -> list[str]:
        if self.config.source.url:
            return [self.config.source.url]

    # def get_content_leaf_names(self) -> list[str]:
    #     if self.config.source.type in (".xlsx", ".xlsb", ".xlsm", ".xls"):
    #         return get_sheet_names(str(self))
    #     # elif self.suffix == ".zip":
    #     #     return get_zip_files(str(self))
    #     elif (
    #         self.is_config_file() and self.config.source.type == "pandas"
    #     ):  # and self._config.type =='mssql'
    #         # return get_db_tables
    #         # pass  # TODO
    #         return list(ee.staged_frames)
    #     elif self.is_config_file():
    #         return []
    #     else:
    #         return [self.stem]

    def is_hidden(self) -> bool:
        """Check if the given Path object is hidden."""
        # Check for UNIX-like hidden files/directories
        if self.name.startswith("."):
            return True

        # Check for Windows hidden files/directories
        if os.name == "nt":
            try:
                attrs = os.stat(self)
                return bool(attrs.st_file_attributes & FILE_ATTRIBUTE_HIDDEN)
            except AttributeError:
                # If FILE_ATTRIBUTE_HIDDEN not defined,
                # assume it's not hidden
                pass

        return False

    @property
    def abs(self) -> Self:
        return ContentAwarePath(self.absolute())

    @property  # fs = filesystem, can return a File or Dir but not content
    def fs(self) -> Optional[Self]:
        if self.is_data_table():
            res = self.parent
        else:
            res = self
        return res

    @property
    def dir(self) -> Optional[Self]:
        if self.is_data_table() and self.parent:
            res = self.parent.dir
        elif self.is_file():
            if self.parent:
                res = self.parent
            else:
                res = Path(self).parent
        else:
            res = self
        return res

    @property
    def file(self) -> Optional[Self]:
        if self.is_data_table():
            res = self.parent
        elif self.is_file():
            res = self
        else:
            res = None
        return res

    @property
    def ext(self) -> str:
        file = self.file
        if file:
            return file.suffix
        else:
            return ""

    @property
    def get_leaf_df(self) -> pd.DataFrame:
        def leaf_to_dict(leaf):
            data = {}
            data["name"] = leaf.name
            data["file_path"] = leaf.config.source.url
            data["type"] = leaf.config.source.type
            data["table"] = leaf.config.target.table
            data["load_parallel"] = leaf.config.source.load_parallel
            data["config"] = leaf.config

            return data

        # raise Exception(self.leaves)
        data = [
            leaf_to_dict(leaf)
            for leaf in self.leaves
            if leaf.node_type == NodeType.DATA_TABLE
        ]
        df = pd.DataFrame(data)
        # raise Exception(self.leaves)
        return df

    @staticmethod
    def apply_file_wrappers(
        parent: Optional[ef.FlowNodeMixin],
        df: pd.DataFrame,
        execute_fn: Callable[[ec.Config], bool],
    ) -> None:
        ingest_files = ef.EelFlow(parent=parent, n_jobs=1)
        for file, file_gb in df.groupby(["file_path", "type"]):
            if file[1] in (".xlsx", ".xls", ".xlsm", ".xlsb"):
                file_wrapper = ef.EelXlsxWrapper(parent=ingest_files, file_path=file[0])
            else:
                file_wrapper = ef.EelFileWrapper(parent=ingest_files, file_path=file[0])
            exe_flow = ef.EelFlow(parent=file_wrapper, n_jobs=1)
            for task_row in file_gb[["name", "config"]].itertuples():
                ef.EelExecute(
                    parent=exe_flow,
                    name=task_row.name,
                    config=task_row.config,
                    execute_fn=execute_fn,
                )

    def get_ingest_taskflow(self) -> ef.EelFlow:
        df = self.get_leaf_df
        root_flow = ef.EelFlow()
        # raise Exception(df.columns)
        if "table" not in df.columns:
            raise Exception("table column not found in leaf dataframe")
        for table, table_gb in df.groupby("table", dropna=False):
            file_group_wrapper = ef.EelFileGroupWrapper(
                parent=root_flow, name=str(table)
            )
            ContentAwarePath.apply_file_wrappers(
                parent=file_group_wrapper, df=table_gb, execute_fn=ee.ingest
            )

        return root_flow

    def get_detect_taskflow(self) -> ef.EelFlow:
        df = self.get_leaf_df
        root_flows = ContentAwarePath.apply_file_wrappers(
            parent=None, df=df, execute_fn=ee.detect
        )
        res = ef.EelFlow(root_flows, 1)
        return res

    def get_eel_yml_preview(self, diff: bool = True) -> list[dict]:
        ymls = []
        # for path, node in self.index.items():
        for node in [node for node in PreOrderIter(self)]:
            if node.node_type != NodeType.CONFIG_VIRTUAL:
                node_config = node.config_raw(True).model_dump(
                    # TODO: excluding load_parallel for demo purposes
                    exclude_none=True,
                    exclude={"source": {"load_parallel"}},
                )
                if node.is_root:
                    save_yml_dict = node_config
                elif diff:
                    if node.parent.node_type != NodeType.CONFIG_VIRTUAL:
                        parent_config = node.parent.config_raw(True).model_dump(
                            exclude_none=True
                        )
                    else:
                        parent_config = node.parent.parent.config_raw(True).model_dump(
                            exclude_none=True
                        )
                    save_yml_dict = dict_diff(parent_config, node_config)
                else:
                    save_yml_dict = node_config
                # if save_yml_dict and node.is_leaf:
                if save_yml_dict:
                    ymls.append(save_yml_dict)
        return ymls
        # save_path = self.root.path / self.CONFIG_PREVIEW_FILE_NAME
        # with save_path.open("w", encoding="utf-8") as file:
        #     yaml.safe_dump_all(ymls, file, sort_keys=False, allow_unicode=True)

    def force_pandas_target(self):
        # iterate all branches and leaves
        for node in PreOrderIter(self):
            # remove target from config
            if type(node._config) is ec.Config:
                node._config.target.url = None
            elif "target" in node._config and "url" in node._config["target"]:
                node._config["target"]["url"] = None

    def set_nrows(self, nrows: int):
        # iterate all branches and leaves
        for node in PreOrderIter(self):
            # remove target from config
            if type(node._config) is ec.Config:
                node._config.source.nrows = nrows
            elif "source" in node._config and "nrows" in node._config["source"]:
                node._config["source"]["nrows"] = None


def dict_diff(dict1: dict, dict2: dict) -> dict:
    """
    Return elements that are in dict2 but not in dict1.

    :param dict1: First dictionary
    :param dict2: Second dictionary
    :return: A dictionary with elements only from dict2 that are not in dict1
    """
    diff = {}

    for key, value in dict2.items():
        # If key is not present in dict1, add the item
        if key not in dict1:
            diff[key] = value
        # If key is present in both dicts and both values are dicts, recurse
        elif isinstance(value, dict) and isinstance(dict1[key], dict):
            nested_diff = dict_diff(dict1[key], value)
            if nested_diff:
                diff[key] = nested_diff
        elif dict1[key] != value:
            diff[key] = value

    return diff


# def get_sheet_names(file_path, sheet_states: list = ["visible"]) -> list[str]:
def get_sheet_names(
    file_path, sheet_states: list = [SheetVisibleEnum.Visible]
) -> list[str]:
    # workbook = load_workbook(
    #     filename=file_path, read_only=True, data_only=True, keep_links=False
    # )
    workbook = CalamineWorkbook.from_path(file_path)

    if sheet_states is None:
        # worksheet_names = workbook.sheetnames
        sheet_states = [SheetVisibleEnum.Visible]
    # else:
    worksheet_names = [
        sheet.name
        # for sheet in workbook.worksheets
        for sheet in workbook.sheets_metadata
        if (sheet.visible in sheet_states) and (sheet.typ == SheetTypeEnum.WorkSheet)
    ]

    # workbook.close()
    return worksheet_names


def get_yml_docs(
    path: Union[ContentAwarePath, Path], expected: int = None
) -> list[dict]:
    if path.exists():
        with path.open() as file:
            yaml_text = file.read()
            documents = list(yaml.safe_load_all(yaml_text))
    # elif str(path).endswith(CONFIG_FILE_EXT):
    #     documents = [{"source": {"url": str(path).removesuffix(CONFIG_FILE_EXT)}}]

    # configs are loaded only to ensure they conform with yml schema
    _ = get_configs(documents)

    if expected is None or len(documents) == expected:
        return documents
    else:
        raise Exception(
            f"unexpected number of documents in {path}; expected: {expected}; found: {len(documents)}"
        )


def get_configs(ymls: list[dict]) -> list[ec.Config]:
    configs = []
    for yml in ymls:
        config = ec.Config(**yml)
        configs.append(config)
    return configs


def get_config_default() -> ec.Config:
    return ec.Config()


def config_path_valid(path: ContentAwarePath) -> bool:
    if path.is_dir():
        return True
    if path.is_file() or ContentAwarePath(path).is_config_file():
        file_type = FileType.suffix_to_type(path.suffix)
        if isinstance(file_type, FileType):
            return True
    return False


def get_content_leaf_names(source: ec.Source) -> list[str]:
    # raise Exception()
    if source.type in (".xlsx", ".xlsb", ".xlsm", ".xls"):
        return get_sheet_names(source.url)
    elif source.type in (".csv", ".tsv", ".fwf", ".xml"):
        # return root file name without path and suffix
        res = [Path(source.url).stem]
        return res
    # elif self.suffix == ".zip":
    #     return get_zip_files(str(self))
    elif source.type == "pandas":  # and self._config.type =='mssql'
        # return get_db_tables
        # pass  # TODO
        return list(ee.staged_frames)
    else:
        return [source.url]
