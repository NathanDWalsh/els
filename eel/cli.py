import typer
import logging
import yaml
from pygments import highlight
from pygments.lexers import YamlLexer
from pygments.formatters import TerminalFormatter
import sys
import os
from pathlib import Path
from typing import Union

import eel.tree as et
from eel.path import ContentAwarePath as CAPath

app = typer.Typer()


def start_logging():
    logging.basicConfig(level=logging.INFO, format="%(relativeCreated)d - %(message)s")
    logging.info("Getting Started")


def plant_tree() -> CAPath:
    root = et.grow_branches()
    logging.info("Tree Created")
    return root


def get_taskflow():
    tree = plant_tree()
    taskflow = tree.get_ingest_taskflow()
    return taskflow


@app.command()
def tree():
    tree = plant_tree()
    tree.display_tree()
    logging.info("Fin")


@app.command()
def flow():
    taskflow = get_taskflow()
    taskflow.display_tree()
    logging.info("Fin")


@app.command()
def execute():
    taskflow = get_taskflow()
    taskflow.execute()
    logging.info("Fin")


@app.command()
def preview():
    root = find_root()
    cwd = Path(os.getcwd())
    if root == cwd:
        tree = plant_tree()
        ymls = tree.get_eel_yml_preview()
        yaml_str = yaml.dump_all(ymls, sort_keys=False, allow_unicode=True)
        if sys.stdout.isatty():
            colored_yaml = highlight(yaml_str, YamlLexer(), TerminalFormatter())
            sys.stdout.write(colored_yaml)
        else:
            sys.stdout.write(yaml_str)
    else:
        print("current path different than eel root")


def find_dir_with_file(start_dir: Path, target_file: str) -> Path:
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
    root = find_dir_with_file(cwd, et.get_root_config_name())
    if not root:
        logging.error("eel root not found")
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
    os.chdir("D:\\test_data2")
    flow()
