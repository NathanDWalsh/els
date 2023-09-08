from pathlib import Path
from anytree import NodeMixin, RenderTree, PreOrderIter
import pandas as pd
import os
import yaml
from typing import Union, Callable
import eel.config as ec
import eel.flow as ef
import eel.execute as ee


class PathToStringMixin:
    @property
    def str(self):
        return str(self)


class ConfigInheritanceMixin:
    @property
    def config_inherited(self) -> ec.Config:
        if self.parent is None:
            return ec.Config()
        else:
            return self.parent.config_combined

    @property
    def config_combined(self) -> ec.Config:
        config_inherited = self.config_inherited.model_copy(deep=True)
        if self._config is None:
            return config_inherited
        else:
            return ConfigInheritanceMixin.merge_configs(config_inherited, self._config)

    @property
    def config(self) -> ec.Config:
        config = self.config_combined.model_copy(deep=True)
        config.sub_path = self.str
        return self.eval_dynamic_attributes(config)

    def get_path_props_find_replace(self) -> dict:
        res = {}
        for member in ec.DynamicPathValue:
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

    @staticmethod
    def merge_configs(*configs: list[Union[ec.Config, dict]]) -> ec.Config:
        dicts = []
        for config in configs:
            if isinstance(config, ec.Config):
                dicts.append(config.model_dump())
            elif isinstance(config, dict):
                dicts.append(config)
            else:
                raise Exception("configs should be a list of Configs or dicts")
        dict_result = ConfigInheritanceMixin.merge_dicts_by_top_level_keys(*dicts)
        res = ec.Config(**dict_result)
        return res

    @staticmethod
    def merge_dicts_by_top_level_keys(*dicts: list[dict]) -> dict:
        merged_dict = {}
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


class ContentAwarePath(
    Path,
    PathToStringMixin,
    ec.HumanPathPropertiesMixin,
    NodeMixin,
    ConfigInheritanceMixin,
):
    _flavour = type(Path())._flavour

    def __init__(self, *args, parent=None, config={}, **kwargs):
        # super().__init__()
        # self._parent = tree_parent
        # NodeMixin().parent = tree_parent
        self.parent = parent
        self._config = config

    def display_tree(self):
        for pre, fill, node in RenderTree(self):
            print("%s%s" % (pre, node.name))

    @property
    def parent(self):
        # return NodeMixin().parent
        return NodeMixin.parent.fget(self)

    @parent.setter
    def parent(self, value):
        NodeMixin.parent.fset(self, value)

    # @property
    def is_content(self) -> bool:
        """
        Check if the path points to a content inside a file.
        A naive check is to see if the parent exists as a file.
        """
        return self.parent.is_file()

    def get_total_files(self) -> int:
        """Return the total number of files in a folder and subfolders."""
        if self.is_dir():
            return sum(
                1 for file in self.rglob("*") if file.is_file() and not file.is_hidden()
            )
        elif self.is_file():
            return 1
        else:
            return 0

    def is_hidden(path: Path) -> bool:
        """Check if the given Path object is hidden."""
        # Check for UNIX-like hidden files/directories
        if path.name.startswith("."):
            return True

        # Check for Windows hidden files/directories
        if os.name == "nt":
            try:
                attrs = os.stat(path)
                return attrs.st_file_attributes & os.FILE_ATTRIBUTE_HIDDEN
            except AttributeError:
                # If FILE_ATTRIBUTE_HIDDEN not defined,
                # assume it's not hidden
                pass

        return False

    @property
    def abs(self) -> "ContentAwarePath":
        return ContentAwarePath(self.absolute())

    @property  # fs = filesystem, can return a File or Dir but not content
    def fs(self) -> "ContentAwarePath":
        if self.is_content():
            res = self.parent
        else:
            res = self
        return res

    @property
    def dir(self) -> "ContentAwarePath":
        if self.is_content():
            res = self.parent.parent
        elif self.is_file():
            res = self.parent
        else:
            res = self
        return res

    @property
    def file(self) -> "ContentAwarePath":
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
            data["file_path"] = leaf.file.abs.str
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
        parent: ef.FlowNodeMixin,
        df: pd.DataFrame,
        execute_fn: Callable[[ec.Config], bool],
    ) -> None:
        ingest_files = ef.EelFlow(parent=parent, n_jobs=1)
        for file, file_gb in df.groupby(["file_path", "type"]):
            if file[1] == ".xlsx":
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
        for _, table_gb in df.groupby("table", dropna=False):
            file_group_wrapper = ef.EelFileGroupWrapper(
                parent=root_flow, exec_parallel=False
            )
            ContentAwarePath.apply_file_wrappers(
                parent=file_group_wrapper, df=table_gb, execute_fn=ee.ingest
            )

        return root_flow

    def get_detect_taskflow(self) -> ef.EelFlow:
        df = self.get_leaf_df
        root_flows = ContentAwarePath.apply_file_wrappers(df, ee.detect)
        res = ef.EelFlow(root_flows, 1)
        return res

    def save_eel_yml_preview(self):
        ymls = []
        # for path, node in self.index.items():
        for node in [node for node in PreOrderIter(self.root)]:
            node_config = node.config.model_dump(exclude_none=True)
            node_config["sub_path"] = node.str
            if node.is_root:
                save_yml_dict = node_config
            else:
                parent_config = node.parent.config.model_dump(exclude_none=True)
                save_yml_dict = dict_diff(parent_config, node_config)
            ymls.append(save_yml_dict)
        save_path = self.root.path / self.CONFIG_PREVIEW_FILE_NAME
        with save_path.open("w", encoding="utf-8") as file:
            yaml.safe_dump_all(ymls, file, sort_keys=False, allow_unicode=True)


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
