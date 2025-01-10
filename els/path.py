import logging
import os
import sys

# from collections.abc import Generator
from enum import Enum
from pathlib import Path
from stat import FILE_ATTRIBUTE_HIDDEN
from typing import Callable, Optional, Union

import pandas as pd
import sqlalchemy as sa
import typer
import yaml

# import zipfile
from anytree import NodeMixin, PreOrderIter, RenderTree

# from openpyxl import load_workbook
from python_calamine import CalamineWorkbook, SheetTypeEnum, SheetVisibleEnum

import els.config as ec
import els.execute as ee
import els.flow as ef
from els.pathprops import HumanPathPropertiesMixin

CONFIG_FILE_EXT = ".els.yml"
FOLDER_CONFIG_FILE_STEM = "_"
ROOT_CONFIG_FILE_STEM = "__"

# config_dict_type: TypeAlias = dict[str, dict[str, str]]


class NodeType(Enum):
    CONFIG_DIRECTORY = "config directory"
    CONFIG_EXPLICIT = "explicit config"
    CONFIG_ADJACENT = "adjacent config"
    CONFIG_VIRTUAL = "virtual config"
    # CONFIG_DOC = "config_doc"
    DATA_URL = "source url"
    DATA_TABLE = "data_table"


class FileType(Enum):
    EXCEL = "excel"
    CSV = "csv"
    ELS = "els"
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
            # TODO: handle double extension els.yml
            # for now assumes any yml file is an els config
            "yml": cls.ELS,
            "fwf": cls.FWF,
            "xml": cls.XML,
        }
        return mapping.get(extension.lower().strip("."), None)


def get_dir_config_name():
    return FOLDER_CONFIG_FILE_STEM + CONFIG_FILE_EXT


def get_root_config_name():
    return ROOT_CONFIG_FILE_STEM + CONFIG_FILE_EXT


class ConfigPath(Path, HumanPathPropertiesMixin, NodeMixin):
    # subclassing pathlib.Path not supported until Python 3.12
    if sys.version_info < (3, 12):
        _flavour = type(Path())._flavour  # type: ignore

    def __init__(
        self,
        *args,
        **kwargs,
    ):
        if sys.version_info < (3, 12):
            pass
        else:
            super().__init__(*args, **kwargs)
        # self.parent = parent

    # called from plant_tree() to build:
    #  (1) individual inheritance chain nodes without walking
    #  (2) configuration context node with walking
    # called from grow_dir_branches() to build
    #  (1) config file nodes
    #  (2) config dir nodes
    def configure_node(
        self,
        walk_dir: Optional[bool] = False,
    ):
        if self.is_dir():
            self._config = self.dir_config

            if walk_dir:
                self.grow_dir_branches()
                if not self.has_leaf_table:
                    # do not add dirs with no leaf nodes which are tables
                    self.parent = None

        elif self.is_config_file:
            self._config = {"source": {"url": self.adjacent_file_path}}
            self.grow_config_branches()

        else:
            raise Exception("Unknown node cannot be configured.")

    def grow_dir_branches(self):
        for subpath in self.glob("*"):
            # ensure node-level configs are not (double) counted
            if (
                subpath.name
                in (
                    get_dir_config_name(),
                    get_root_config_name(),
                )
                or subpath in self.children
                or ConfigPath(str(subpath) + CONFIG_FILE_EXT) in self.children
            ):
                pass
            elif config_path_valid(subpath):
                if subpath.is_dir() or subpath.is_config_file():  # adjecent config
                    cpath = subpath
                else:  # directory or explicit config file
                    cpath = ConfigPath(str(subpath) + CONFIG_FILE_EXT)
                cpath.parent = self
                cpath.configure_node(walk_dir=True)
            else:
                logging.warning(f"Invalid path not added to tree: {str(subpath)}")

    @property
    def dir_config(self):
        configs = []

        # a dir can have root config and/or dir config
        if self.is_root_dir:
            config_path = Path(self) / get_root_config_name()
            if config_path.exists():
                ymls = get_yml_docs(config_path, expected=1)
                configs.append(ymls[0])

        if self.is_dir():
            config_path = Path(self) / get_dir_config_name()
            if config_path.exists():
                ymls = get_yml_docs(config_path, expected=1)
                configs.append(ymls[0])
            # if both root and dir config found, merge
            if len(configs) > 0:
                return ConfigPath.merge_configs(*configs)
            else:
                return {}
        else:
            raise Exception("dir_config called on a non-directory node.")

    @property
    def paired_config(self):
        if self.node_type != NodeType.CONFIG_VIRTUAL:
            docs = get_yml_docs(self)

            # adjacent can have an explicit url if it matches adjacent
            # (TODO test all documents instead of just the first)
            if (
                self.node_type == NodeType.CONFIG_ADJACENT
                and "source" in docs[0]
                and "url" in docs[0]["source"]
                and docs[0]["source"]["url"] != self.adjacent_file_path
            ):
                raise Exception(
                    f"adjacent config {self} has url: {docs[0]['source']['url']} "
                    "different than its adjacent data file: "
                    f"{self.adjacent_file_path}"
                )

            return docs
        else:  # NodeType.CONFIG_VIRTUAL has no explicit config
            return [dict()]

    def grow_config_branches(self):
        previous_url = ""
        # raise Exception(self.paired_config)
        for doc in self.paired_config:
            merged_doc = ConfigPath.merge_configs(self.config, doc)
            source = merged_doc.source

            if source.url and source.url != previous_url:
                previous_url = source.url
                url_parent = ConfigPath(Path(previous_url))
                url_parent.parent = self
                url_parent._config = doc

            if previous_url == "":
                raise Exception("expected to have a url for child config doc")

            table_docs = dict()
            if self.node_type in (
                NodeType.CONFIG_ADJACENT,
                NodeType.CONFIG_VIRTUAL,
            ) or (source.type_is_db and not source.table):
                for content_table in get_content_leaf_names(url_parent.config.source):
                    if not source.table or source.table == content_table:
                        doc = ConfigPath.merge_configs(
                            doc, {"source": {"table": content_table}}
                        )
                        table_docs[content_table] = doc
            else:
                # if no source table defined explicitly, assumes to be last element in url
                # (after last / and (before first .))
                # TODO: consider relocating to config
                if not source.table:
                    source.table = source.url.split("/")[-1].split(".")[0]
                table_docs[source.table] = doc

            for tab, doc in table_docs.items():
                ca_path = ConfigPath(Path(previous_url) / tab)
                ca_path.parent = url_parent
                ca_path._config = doc

    @property
    def node_type(self) -> NodeType:
        if self.is_dir():
            return NodeType.CONFIG_DIRECTORY
        elif self.is_config_file():
            if self.is_file():
                if Path(str(self).replace(CONFIG_FILE_EXT, "")).is_file():
                    return NodeType.CONFIG_ADJACENT
                else:
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
    # def get_leaf_tables(self) -> list[Self]:
    def get_leaf_tables(self):
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
                return f"{self.abs}\\{get_root_config_name()}"
            else:
                return f"{self.abs}\\{get_dir_config_name()}"
        elif (
            self.node_type == NodeType.CONFIG_EXPLICIT
            or self.node_type == NodeType.CONFIG_VIRTUAL
            or self.node_type == NodeType.CONFIG_ADJACENT
        ):
            return str(self.abs)
        elif self.node_type == NodeType.DATA_URL:
            return str(self.parent.abs)
        elif self.node_type == NodeType.DATA_TABLE:
            return str(self.parent.parent.abs)

    def config_raw(self, add_config_file_path=False) -> ec.Config:
        config_line = []
        # if root els config is mandatory, this "default dump line" is not required
        config_line.append(ec.Config().model_dump(exclude_none=True))

        for node in self.ancestors + (self,):
            if node._config:
                if isinstance(node._config, ec.Config):
                    config_line.append(node._config.model_dump(exclude_none=True))
                else:
                    config_line.append(node._config)

        config_merged = ConfigPath.merge_configs(*config_line)
        config_copied = config_merged.model_copy(deep=True)
        if add_config_file_path:
            config_copied.config_path = self.config_file_path

        return config_copied

    @property
    def config(self) -> ec.Config:
        config_copied = self.config_raw()

        # if self.is_leaf:
        config_evaled = self.eval_dynamic_attributes(config_copied)
        # else:
        #     config_evaled = config_copied

        if self.node_type == NodeType.DATA_TABLE:
            if not config_evaled.target.if_exists:
                config_evaled.target.if_exists = ec.TargetIfExistsValue.FAIL

            if not config_evaled.target.table:
                config_evaled.target.table = self.name

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
        ConfigPath.swap_dict_vals(config_dict, find_replace)
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
                ConfigPath.swap_dict_vals(dictionary[key], find_replace_dict)
            elif isinstance(value, list):
                pass
            elif value in find_replace_dict:
                dictionary[key] = find_replace_dict[value]
            # elif key == "url" and "*" in value:
            #     dictionary[key] = value.replace("*", find_replace_dict["_leaf_name"])

    @staticmethod
    def merge_configs(*configs: list[Union[ec.Config, dict]]) -> ec.Config:
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
        dict_result = ConfigPath.merge_dicts_by_top_level_keys(*dicts)
        res = ec.Config(**dict_result)  # type: ignore
        return res

    @staticmethod
    def merge_dicts_by_top_level_keys(*dicts: dict) -> dict:
        merged_dict: dict = {}
        for dict_ in dicts:
            for key, value in dict_.items():
                if (
                    key in merged_dict
                    and isinstance(value, dict)
                    and (merged_dict[key] is not None)
                ):
                    merged_dict[key].update(value)
                elif value is not None:
                    # Add a new key-value pair to the merged dictionary
                    merged_dict[key] = value
        return merged_dict

    @property
    def is_root_dir(self):
        return self.is_dir() and self.is_root

    @property
    def adjacent_file_path(self):
        return str(Path(str(self).replace(CONFIG_FILE_EXT, "")))

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
        # if (self.is_dir and not self.has_leaf_table) or (self in self.siblings):
        #     # do not add dirs with no leaf nodes which are tables
        #     # TODO this could be changed to search for config files instead ...
        #     # ... making debugging faulty config files easier
        #     # pass
        #     setval = None
        # else:
        #     setval = value
        if NodeMixin.parent.fset:
            NodeMixin.parent.fset(self, value)

    @property
    def root_node(self):
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
    # def abs(self) -> Self:
    def abs(self):
        return ConfigPath(self.absolute())

    @property  # fs = filesystem, can return a File or Dir but not content
    # def fs(self) -> Optional[Self]:
    def fs(self):
        if self.is_data_table():
            res = self.parent
        else:
            res = self
        return res

    @property
    # def dir(self) -> Optional[Self]:
    def dir(self):
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
    # def file(self) -> Optional[Self]:
    def file(self):
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
        ingest_files = ef.ElsFlow(parent=parent, n_jobs=1)
        for file, file_gb in df.groupby(["file_path", "type"]):
            if file[1] in (".xlsx", ".xls", ".xlsm", ".xlsb"):
                file_wrapper = ef.ElsXlsxWrapper(parent=ingest_files, file_path=file[0])
            else:
                file_wrapper = ef.ElsFileWrapper(parent=ingest_files, file_path=file[0])
            exe_flow = ef.ElsFlow(parent=file_wrapper, n_jobs=1)
            for task_row in file_gb[["name", "config"]].itertuples():
                ef.ElsExecute(
                    parent=exe_flow,
                    name=task_row.name,
                    config=task_row.config,
                    execute_fn=execute_fn,
                )

    def get_ingest_taskflow(self) -> ef.ElsFlow:
        df = self.get_leaf_df
        root_flow = ef.ElsFlow()
        # raise Exception(df.columns)
        if "table" not in df.columns:
            raise Exception("table column not found in leaf dataframe")
        for table, table_gb in df.groupby("table", dropna=False):
            file_group_wrapper = ef.ElsFileGroupWrapper(
                parent=root_flow, name=str(table)
            )
            ConfigPath.apply_file_wrappers(
                parent=file_group_wrapper, df=table_gb, execute_fn=ee.ingest
            )

        return root_flow

    def get_detect_taskflow(self) -> ef.ElsFlow:
        df = self.get_leaf_df
        root_flows = ConfigPath.apply_file_wrappers(
            parent=None, df=df, execute_fn=ee.detect
        )
        res = ef.ElsFlow(root_flows, 1)
        return res

    def get_els_yml_preview(self, diff: bool = True) -> list[dict]:
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


def get_root_inheritance(start_dir: Path) -> Union[list[Path], None]:
    if start_dir:
        start_dir = Path(start_dir)
    else:
        start_dir = Path()

    dirs = []
    current_dir = start_dir.absolute()
    file_found = False

    while (
        current_dir != current_dir.parent
    ):  # This condition ensures we haven't reached the root
        dirs.append(current_dir)
        if (current_dir / get_root_config_name()).exists():
            file_found = True
            break
        current_dir = current_dir.parent

    # Check and add the root directory if not already added
    if current_dir not in dirs and (current_dir / get_root_config_name()).exists():
        dirs.append(current_dir)
        file_found = True
    if file_found:
        # print(dirs)
        return dirs
    else:
        glob_pattern = "**/*" + get_root_config_name()
        below = sorted(start_dir.glob(glob_pattern))
        if len(below) > 0:
            return [Path(below[0].parent.absolute())]
        else:
            logging.info(f"els root not found, using {start_dir}")
            if (
                start_dir.is_file()
                and (start_dir.parent / get_dir_config_name()).exists()
            ):
                return [start_dir, start_dir.parent]
            elif (
                not start_dir.exists()
                and (start_dir.parent / get_dir_config_name()).exists()
            ):
                return [start_dir, start_dir.parent]
            else:
                return [start_dir]


def plant_tree(path: ConfigPath) -> Optional[ConfigPath]:
    root_paths = list(reversed(get_root_inheritance(str(path))))
    root_path = Path(root_paths[0])
    if root_path.is_dir():
        os.chdir(root_path)
        # TODO: understand this better
        # seemingly redundant lines below fix strange bug when passing a directory as an
        # argument it got duplicated in the path, i.e. /foo/bar/bar when just /foo/bar
        # expected
        root_path = Path()
        root_paths[0] = Path()
    else:
        os.chdir(root_path.parent)
    parent = None
    for index, path_ in enumerate(root_paths):
        if config_path_valid(path_):
            ca_path = ConfigPath(path_)
            ca_path.parent = parent
            # for the nodes in-between context and root, don't walk_dir
            if index < len(root_paths) - 1:
                ca_path.configure_node()
                parent = ca_path
            else:  # For the last item always process configs
                # ca_path = ContentAwarePath(path_)
                # ca_path.parent = parent
                ca_path.configure_node(walk_dir=True)
                # raise Exception(ca_path.children[0].children[0].children)
        else:
            raise Exception("Invalid file in explicit path: " + str(path_))
    logging.info("Tree Created")
    root = parent.root_node if parent else ca_path
    if root.is_leaf and root.is_dir():
        logging.error("Root is an empty directory")
    return root


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


def get_table_names(source: ec.Source) -> list[str]:
    res = None
    if source.type_is_db and not source.table:
        with sa.create_engine(source.db_connection_string).connect() as sqeng:
            inspector = sa.inspect(sqeng)
            res = inspector.get_table_names(source.dbschema)
    return res


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


def get_yml_docs(path: Union[ConfigPath, Path], expected: int = None) -> list[dict]:
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


def config_path_valid(path: ConfigPath) -> bool:
    if path.is_dir():
        return True
    if path.is_file() or ConfigPath(path).is_config_file():
        file_type = FileType.suffix_to_type(path.suffix)
        if isinstance(file_type, FileType):
            return True
    return False


def get_content_leaf_names(source: ec.Source) -> list[str]:
    # raise Exception()
    if source.type_is_db:
        return get_table_names(source)
    elif source.type in (".xlsx", ".xlsb", ".xlsm", ".xls"):
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
