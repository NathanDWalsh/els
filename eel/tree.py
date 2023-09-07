import os
import zipfile
import yaml
from openpyxl import load_workbook
import logging
import re

from anytree import RenderTree
from typing import Optional

from eel.path import ContentAwarePath as CAPath
import eel.config as ec


class EelTree:
    CONFIG_FILE_EXT = ".eel.yml"
    FOLDER_CONFIG_FILE_STEM = "_"
    CONFIG_PREVIEW_FILE_NAME = "_preview.eel.yml"

    def __init__(self, path: CAPath) -> None:
        self.populate_tree(path)

    def populate_tree(self, path: CAPath, parent: CAPath = None) -> None:
        if path.is_dir() and path.get_total_files() > 0:
            config = self.get_paired_config(path)
            folder = self.add_node2(path, parent, config)
            ignore_configs = [
                path / self.folder_config_name,
                CAPath() / self.CONFIG_PREVIEW_FILE_NAME,
            ]
            for path_item in path.iterdir():
                if not path_item.str.endswith(self.CONFIG_FILE_EXT):
                    ignore_configs.append(CAPath(path_item.str + self.CONFIG_FILE_EXT))
                    self.populate_tree(path_item, parent=folder)
                elif path_item not in ignore_configs:
                    # TODO complete
                    logging.error("ERROR: sole ymls not covered: " + path_item.str)
        elif path.is_file() and not path.is_hidden():
            config = self.get_paired_config(path)
            file = self.add_node2(path, parent, config)
            if path.suffix == ".xlsx":
                sheet_deets = get_sheeet_deets(path.str)
                for ws_name, ws_size in sheet_deets.items():
                    config = self.get_paired_config(path, ws_name)
                    self.add_node2(file / ws_name, file, config)
            else:
                # file_size = path.stat().st_size
                self.add_node2(file / file.stem, file, {})  # size=file_size

    def add_node2(self, node: CAPath, parent: CAPath, config: dict) -> CAPath:
        res = CAPath(node.str, parent=parent, config=config)
        if parent is None:
            self.root = res
        return res

    @property
    def folder_config_name(self):
        return self.FOLDER_CONFIG_FILE_STEM + self.CONFIG_FILE_EXT

    def display_tree(self, item_path=None):
        for pre, fill, node in RenderTree(self.root):
            print("%s%s" % (pre, node.name))

    def save_eel_yml_preview(self):
        ymls = []
        for path, node in self.index.items():
            node_config = node.config.model_dump(exclude_none=True)
            node_config["sub_path"] = path
            if node.parent is None:
                save_yml_dict = node_config
            else:
                parent_config = node.parent.config.model_dump(exclude_none=True)
                save_yml_dict = dict_diff(parent_config, node_config)
            ymls.append(save_yml_dict)
        save_path = self.root.path / self.CONFIG_PREVIEW_FILE_NAME
        with save_path.open("w", encoding="utf-8") as file:
            yaml.safe_dump_all(ymls, file, sort_keys=False, allow_unicode=True)

    def get_paired_config(self, item_path: CAPath, sub_path: str = ".") -> dict:
        config_path = self.get_paired_config_path(item_path)
        if config_path:
            ymls = get_yml_docs(config_path)
            configs = get_configs(ymls)
            for config, yml in zip(configs, ymls):
                if sub_path == config.sub_path:
                    return yml
            return {}

    def get_paired_config_path(self, path: CAPath) -> Optional[CAPath]:
        if path.is_dir():
            config_path = path / self.folder_config_name
        elif path.is_file():
            if path.str.endswith(self.CONFIG_FILE_EXT):
                return path
            else:
                config_path = CAPath(path.str + self.CONFIG_FILE_EXT)
        if config_path.exists():
            return config_path
        return None


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


def natural_sort_key(s):
    return [
        int(text) if text.isdigit() else text.lower() for text in re.split(r"(\d+)", s)
    ]


def get_sheet_sizes(file_path):
    sheet_sizes = []

    with zipfile.ZipFile(file_path, "r") as zip_file:
        sheet_names = zip_file.namelist()

        for sheet_name in sorted(sheet_names, key=natural_sort_key):
            if sheet_name.startswith("xl/worksheets/sheet"):
                file_info = zip_file.getinfo(sheet_name)
                sheet_sizes.append(file_info.file_size)

    return sheet_sizes


def get_sheet_names(file_path, sheet_states: list = ["visible"]):
    workbook = load_workbook(
        filename=file_path, read_only=True, data_only=True, keep_links=False
    )

    if sheet_states is None:
        worksheet_names = workbook.sheetnames
    else:
        worksheet_names = [
            sheet.title
            for sheet in workbook.worksheets
            if sheet.sheet_state in sheet_states
        ]

    workbook.close()
    return worksheet_names


def get_sheeet_deets(file_path):
    names = get_sheet_names(file_path)
    # sizes = get_sheet_sizes(file_path)

    return dict(zip(names, names))


def get_yml_docs(path: CAPath) -> list[dict]:
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


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(relativeCreated)d - %(message)s")
    logging.info("Getting Started")

    os.chdir("D:\\test_data2")
    base_path = CAPath()

    ft = EelTree(base_path)
    logging.info("Tree Created")
    ft.display_tree()

    # print(type(ec.Config(**ft.base.first_leaf.config.model_dump())))
    # print(ec.Config(**ft.base.first_leaf.config.model_dump()))

    # print(ft.base.first_leaf.config)
    # print(ft.base.config.eval_dynamic_attributes())

    # ft.save_eel_yml_preview()

    taskflow = ft.root.get_ingest_taskflow()
    print(taskflow.to_tuple)
    taskflow.execute()

    logging.info("Fin")
