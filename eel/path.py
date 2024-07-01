from pathlib import Path

# import zipfile
from anytree import NodeMixin, RenderTree, PreOrderIter
import pandas as pd
import os
from stat import FILE_ATTRIBUTE_HIDDEN
from typing import Union, Callable, Optional, TypeAlias, Self
from collections.abc import Generator
from enum import Enum

# from openpyxl import load_workbook
from python_calamine import CalamineWorkbook, SheetTypeEnum, SheetVisibleEnum
import yaml
import logging

import eel.config as ec
import eel.flow as ef
import eel.execute as ee
from eel.pathprops import HumanPathPropertiesMixin

CONFIG_FILE_EXT = ".eel.yml"
FOLDER_CONFIG_FILE_STEM = "_"
ROOT_CONFIG_FILE_STEM = "__"

config_dict_type: TypeAlias = dict[str, dict[str, str]]


class ConfigType(Enum):
    DIRECTORY = "directory"
    FILE_EXPLICIT = "file_explicit"
    FILE_IMPLICIT = "file_implicit"
    FILE_MIXED = "file_mixed"


class FileType(Enum):
    EXCEL = "excel"
    CSV = "csv"

    @classmethod
    def get(cls, extension: str):
        mapping = {
            "xlsx": cls.EXCEL,
            "xls": cls.EXCEL,
            "xlsm": cls.EXCEL,
            "xlsb": cls.EXCEL,
            "csv": cls.CSV,
        }
        return mapping.get(extension, None)


def get_folder_config_name():
    return FOLDER_CONFIG_FILE_STEM + CONFIG_FILE_EXT


def get_root_config_name():
    return ROOT_CONFIG_FILE_STEM + CONFIG_FILE_EXT


class ContentAwarePath(Path, HumanPathPropertiesMixin, NodeMixin):
    _flavour = type(Path())._flavour  # type: ignore

    def __init__(self, *args, parent: Optional[Self] = None, **kwargs):
        self.parent = parent
        self._config = self.get_paired_config()

    @property
    def config(self) -> ec.Config:
        config_line = []
        # if root eel config is mandatory, this "default dump line" is not required
        config_line.append(ec.Config().model_dump(exclude_none=True))

        for node in self.ancestors + (self,):
            if node._config:
                config_line.append(node._config)
        config_merged = ContentAwarePath.merge_configs(*config_line)
        config_copied = config_merged.model_copy(deep=True)
        # if self.is_leaf:
        config_evaled = self.eval_dynamic_attributes(config_copied)
        # else:
        #     config_evaled = config_copied
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
            elif key == "url" and "*" in value:
                dictionary[key] = value.replace("*", find_replace_dict["_leaf_name"])

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

    def get_paired_config(self) -> config_dict_type:
        # Optional[config_dict_type]:
        if (
            self.parent
            and self.parent._config
            and ("children" in self.parent._config)
            and isinstance(self.parent._config["children"], dict)
            # and len(self.parent._config["children"]) > 0
            # and isinstance(self.parent._config["children"][0], dict)
        ):
            # check if leaf_name is in one of the keys in the dicts contained in list
            if str(self.leaf_name) in self.parent._config["children"]:
                return self.parent._config["children"][str(self.leaf_name)]
            else:
                logging.error("Config not found in parent config when expected")
                return {}
        elif not self.is_content():
            config_path = self.get_paired_config_path()
            if config_path:
                ymls = get_yml_docs(config_path)
                # configs are loaded only to ensure they conform with yml schema
                configs = get_configs(ymls)
                if len(configs) > 1:
                    logging.error(
                        "Found more than one yml document, using first one only"
                    )
                yml = ymls[0]
                if self.is_file() and (
                    (str(self) != str(config_path))
                    or (self.ext in FileType.__members__)
                ):
                    if "source" not in yml:
                        yml["source"] = dict()
                    yml["source"]["url"] = str(self)
                return yml
            else:
                return {"source": {"url": str(self)}}
        else:
            return {}

    def get_paired_config_path(self) -> Optional[Path]:
        # order matters, file is checked first in case a single data file passed
        if self.is_file():
            if str(self).endswith(CONFIG_FILE_EXT):
                return self
            else:
                config_path = Path(str(self) + CONFIG_FILE_EXT)
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
            column1 = f"{pre}{node.name}"
            column2 = ""
            if node.is_leaf and node.config.target.url:
                rel_path = os.path.relpath(node.config.target.url)
                column2 = f" → {rel_path}"
            # elif node.is_leaf and node.config.source.url:
            elif node.is_leaf and node.config.target.type == "pandas":
                column2 = f" → staged_frames['{node.config.target.table}']"

            rows.append((column1, column2))

            column1_width = max(column1_width, len(column1))
            column2_width = max(column2_width, len(column2))

        for column1, column2 in rows:
            print(f"{column1:{column1_width}}{column2:{column2_width}}")

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
    def is_content(self) -> bool:
        """
        Check if the path points to a content inside a file.
        A naive check is to see if the parent exists as a file.
        """
        if self.is_root or not self.parent:
            return False
        else:
            return self.parent.is_file()

    def is_config(self) -> bool:
        return str(self).endswith(CONFIG_FILE_EXT)

    @property
    def subdir_patterns(self) -> list[str]:
        # TODO patterns may overlap
        children = (
            self._config["children"]
            if self._config and "children" in self._config
            else None
        )
        if children is None:
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

    def count_recursive_files_in_subdirs(self) -> int:
        count = 0
        # , subdir_patterns: Union[List[str], None, str, dict]

        for pattern in self.subdir_patterns:
            for subpath in self.glob(pattern):
                if subpath.is_dir():
                    count += sum(
                        1
                        for file in subpath.rglob("*")
                        if file.is_file() and not file.is_hidden()
                    )
                elif subpath.is_file() and not subpath.is_hidden():
                    count += 1
        return count

    def iterbranch(self) -> Generator[Self, None, None]:
        if self.is_dir():
            skip_paths = []
            skip_paths.append(self / get_folder_config_name())
            skip_paths.append(self / get_root_config_name())
            # dirs allow glob matches
            for pattern in self.subdir_patterns:
                for subpath in self.glob(pattern):
                    subpath._config = subpath.get_paired_config()
                    if (subpath not in skip_paths) and (
                        (subpath.is_file() and not subpath.is_hidden())
                        or (subpath.count_recursive_files_in_subdirs())
                    ):  # not str(subpath).endswith(CONFIG_FILE_EXT) and
                        # paths will not repeat due to overlapping globs
                        skip_paths.append(subpath)
                        # do not yield a paired yml
                        # TODO: assumes pathlib sorts ascending
                        skip_paths.append(
                            ContentAwarePath(str(subpath) + CONFIG_FILE_EXT)
                        )
                        yield subpath
        # elif self.is_config():
        #     if self.config.source:
        #         # TODO: elaborate on this, db connection for example
        #         yield self.source
        #     else:
        #         yield self / "memory"
        elif self.is_file():
            # content allows exact matches
            leaf_names = self.get_content_leaf_names()

            for pattern in self.subdir_patterns:
                if pattern == "*":
                    for leaf in leaf_names:
                        yield ContentAwarePath(self / leaf, parent=self)
                    break
                if pattern in leaf_names:
                    yield ContentAwarePath(self / pattern, parent=self)

    def get_content_leaf_names(self) -> list[str]:
        if self.suffix in (".xlsx", ".xlsb", ".xlsm", ".xls"):
            return get_sheet_names(str(self))
        # elif self.suffix == ".zip":
        #     return get_zip_files(str(self))
        elif (
            self.is_config() and self.config.source.type == "pandas"
        ):  # and self._config.type =='mssql'
            # return get_db_tables
            # pass  # TODO
            return list(ee.staged_frames)
        elif self.is_config():
            return []
        else:
            return [self.stem]

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
        if self.is_content():
            res = self.parent
        else:
            res = self
        return res

    @property
    def dir(self) -> Optional[Self]:
        if self.is_content() and self.parent:
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
        if self.is_content():
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
            data["file_path"] = str(leaf.file.abs)
            data["type"] = leaf.config.source.type
            data["table"] = leaf.config.target.table
            data["load_parallel"] = leaf.config.source.load_parallel
            data["config"] = leaf.config

            return data

        data = [leaf_to_dict(leaf) for leaf in self.leaves]
        df = pd.DataFrame(data)
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
        for table, table_gb in df.groupby("table", dropna=False):
            file_group_wrapper = ef.EelFileGroupWrapper(
                parent=root_flow, name=str(table), exec_parallel=False
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
            node_config = node.config.model_dump(exclude_none=True)
            if node.is_root:
                save_yml_dict = node_config
            elif diff:
                parent_config = node.parent.config.model_dump(exclude_none=True)
                save_yml_dict = dict_diff(parent_config, node_config)
            else:
                save_yml_dict = node_config
            if save_yml_dict and node.is_leaf:
                ymls.append(save_yml_dict)
        return ymls
        # save_path = self.root.path / self.CONFIG_PREVIEW_FILE_NAME
        # with save_path.open("w", encoding="utf-8") as file:
        #     yaml.safe_dump_all(ymls, file, sort_keys=False, allow_unicode=True)


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


# def get_zip_files(file_path) -> list[str]:
#     with zipfile.ZipFile(file_path, "r") as zip_ref:
#         zip_files = zip_ref.namelist()
#     return zip_files


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


def get_yml_docs(path: Union[ContentAwarePath, Path]) -> list[dict]:
    with path.open() as file:
        yaml_text = file.read()
        documents = list(yaml.safe_load_all(yaml_text))
    return documents


def get_configs(ymls: list[dict]) -> list[ec.Config]:
    configs = []
    for yml in ymls:
        config = ec.Config(**yml)
        configs.append(config)
    return configs


def get_config_default() -> ec.Config:
    return ec.Config()


def grow_branches(
    path: ContentAwarePath = ContentAwarePath(),
    parent: Optional[ContentAwarePath] = None,
) -> Optional[ContentAwarePath]:
    # if path.is_config():
    #     return node
    if path.is_dir() or path.is_file():
        node = ContentAwarePath(path, parent=parent)
        for path_item in node.iterbranch():
            grow_branches(path_item, parent=node)
        if node.is_leaf and node.is_root and node.is_dir():
            logging.error("Root is an empty directory")
            return None
        else:
            return node
    else:
        return None
    # TODO: consider how database connections / ymls will be handled
