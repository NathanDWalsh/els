import zipfile
import yaml
from openpyxl import load_workbook
import logging
import re

from typing import Optional

from eel.path import ContentAwarePath as CAPath
import eel.config as ec


class EelTree:
    CONFIG_FILE_EXT = ".eel.yml"
    FOLDER_CONFIG_FILE_STEM = "_"
    CONFIG_PREVIEW_FILE_NAME = "_preview.eel.yml"

    def __init__(self, path: CAPath = CAPath()) -> None:
        self.root = None
        self.grow_tree(path)

    def grow_tree(self, path: CAPath, parent: CAPath = None) -> None:
        if path.is_dir() and path.get_total_files() > 0:
            config = EelTree.get_paired_config(path)
            folder = self.add_node(path, parent, config)
            ignore_configs = [
                path / self.get_folder_config_name(),
                CAPath() / self.CONFIG_PREVIEW_FILE_NAME,
            ]
            for path_item in folder.iterdir():
                if not path_item.str.endswith(self.CONFIG_FILE_EXT):
                    ignore_configs.append(CAPath(path_item.str + self.CONFIG_FILE_EXT))
                    self.grow_tree(path_item, parent=folder)
                elif path_item not in ignore_configs:
                    # TODO
                    logging.error("ERROR: sole ymls not covered: " + path_item.str)
        elif path.is_file() and not path.is_hidden():
            config = EelTree.get_paired_config(path)
            file = self.add_node(path, parent, config)
            self.grow_tree_leaves(file)

    def grow_tree_leaves(self, path: CAPath) -> None:
        if path.suffix == ".xlsx":
            sheet_names = get_sheet_names(path.str)
            for ws_name in sheet_names:
                config = EelTree.get_paired_config(path, ws_name)
                self.add_node(path / ws_name, path, config)
        else:
            config = {}
            self.add_node(path / path.stem, path, config)  # size=file_size

    def add_node(self, node: CAPath, parent: CAPath, config: dict) -> CAPath:
        res = CAPath(node.str, parent=parent, config=config)
        if parent is None:
            self.root = res
        return res

    @staticmethod
    def get_folder_config_name():
        return EelTree.FOLDER_CONFIG_FILE_STEM + EelTree.CONFIG_FILE_EXT

    @staticmethod
    def get_paired_config(item_path: CAPath, sub_path: str = ".") -> dict:
        config_path = EelTree.get_paired_config_path(item_path)
        if config_path:
            ymls = get_yml_docs(config_path)
            configs = get_configs(ymls)
            for config, yml in zip(configs, ymls):
                if sub_path == config.sub_path:
                    return yml
            return {}

    @staticmethod
    def get_paired_config_path(path: CAPath) -> Optional[CAPath]:
        if path.is_dir():
            config_path = path / EelTree.get_folder_config_name()
        elif path.is_file():
            if path.str.endswith(EelTree.CONFIG_FILE_EXT):
                return path
            else:
                config_path = CAPath(path.str + EelTree.CONFIG_FILE_EXT)
        if config_path.exists():
            return config_path
        return None


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
