import typer
import logging
import yaml
from pygments import highlight
from pygments.lexers import YamlLexer
from pygments.formatters import TerminalFormatter
import sys
import os
from pathlib import Path
from typing import Union, Optional

from eel.path import ContentAwarePath as CAPath
from eel.path import get_root_config_name
from eel.path import grow_branches
from eel.path import get_config_default
from eel.execute import pandas_end_points

app = typer.Typer()


def start_logging():
    logging.basicConfig(level=logging.INFO, format="%(relativeCreated)d - %(message)s")
    logging.info("Getting Started")


def plant_tree() -> Optional[CAPath]:
    root = grow_branches()
    logging.info("Tree Created")
    return root


def get_taskflow():
    tree = plant_tree()
    if tree:
        taskflow = tree.get_ingest_taskflow()
        return taskflow
    else:
        return None


@app.command()
def tree():
    tree = plant_tree()
    if tree:
        tree.display_tree()
    else:
        logging.error("tree not loaded")
    logging.info("Fin")


@app.command()
def flow():
    taskflow = get_taskflow()
    if taskflow:
        taskflow.display_tree()
    else:
        logging.error("taskflow not loaded")
    logging.info("Fin")


@app.command()
def execute():
    taskflow = get_taskflow()
    if taskflow:
        taskflow.execute()
        print(pandas_end_points)
    else:
        logging.error("taskflow not loaded")
    logging.info("Fin")


def write_yaml_str(yaml_str):
    if sys.stdout.isatty():
        colored_yaml = highlight(yaml_str, YamlLexer(), TerminalFormatter())
        sys.stdout.write(colored_yaml)
    else:
        sys.stdout.write(yaml_str)


@app.command()
def test():
    config_default = get_config_default()
    yml = config_default.model_dump(exclude_none=True)
    yaml_str = yaml.dump(yml, sort_keys=False, allow_unicode=True)
    write_yaml_str(yaml_str)


@app.command()
def preview(verbose: bool = False):
    root = find_root()
    cwd = Path(os.getcwd())
    if root == cwd:
        tree = plant_tree()
        if tree and verbose:
            ymls = tree.get_eel_yml_preview(diff=False)
        elif tree:
            ymls = tree.get_eel_yml_preview(diff=True)
        else:
            raise Exception("tree not loaded")
        yaml_str = yaml.dump_all(ymls, sort_keys=False, allow_unicode=True)
        write_yaml_str(yaml_str)
    else:
        print("current path different than eel root")


def find_dir_with_file(start_dir: Path, target_file: str) -> Optional[Path]:
    current_dir = start_dir
    while (
        current_dir != current_dir.parent
    ):  # This condition ensures we haven't reached the root
        if (current_dir / target_file).exists():
            return current_dir
        current_dir = current_dir.parent
    # Check for the root directory
    if (current_dir / target_file).exists():
        return current_dir
    return None


def find_root() -> Union[Path, None]:
    cwd = Path(os.getcwd())
    root = find_dir_with_file(cwd, get_root_config_name())
    if not root:
        logging.info("eel root not found, using cwd")
        root = cwd
    if not root:
        logging.error("unknown error, root not found")
        return None
    return root


@app.command()
def root():
    root = find_root()
    print(root)


def main():
    start_logging()
    app()


if __name__ == "__main__":
    start_logging()
    os.chdir("D:\\Sync\\repos\\eel\\temp")
    execute()
    print(list(pandas_end_points.values())[0].dtypes)
