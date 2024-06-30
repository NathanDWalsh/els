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

from eel.execute import staged_frames

app = typer.Typer()


def start_logging():
    logging.basicConfig(level=logging.INFO, format="%(relativeCreated)d - %(message)s")
    logging.info("Getting Started")


def plant_tree(path: CAPath) -> Optional[CAPath]:
    root = grow_branches(path)
    logging.info("Tree Created")
    return root


def get_ca_path(path: str = None) -> CAPath:
    if path:
        ca_path = CAPath(path)
    else:
        ca_path = CAPath()
    return ca_path


def get_taskflow(path: str = None):
    ca_path = get_ca_path(path)
    tree = plant_tree(ca_path)
    if tree:
        taskflow = tree.get_ingest_taskflow()
        return taskflow
    else:
        return None


@app.command()
def tree(path: Optional[str] = typer.Argument(None)):
    path = clean_none_path(path)
    ca_path = get_ca_path(path)
    tree = plant_tree(ca_path)
    if tree:
        tree.display_tree()
    else:
        logging.error("tree not loaded")
    logging.info("Fin")


@app.command()
def flow(path: Optional[str] = typer.Argument(None)):
    path = clean_none_path(path)
    taskflow = get_taskflow(path)
    if taskflow:
        taskflow.display_tree()
    else:
        logging.error("taskflow not loaded")
    logging.info("Fin")


def clean_none_path(path):
    if isinstance(path, typer.models.ArgumentInfo) and path.default is None:
        path = None
    return path


@app.command()
def execute(path: Optional[str] = typer.Argument(None)):
    path = clean_none_path(path)
    taskflow = get_taskflow(path)
    if taskflow:
        taskflow.execute()
        # print(pandas_end_points)
    else:
        logging.error("taskflow not loaded")
    if staged_frames:
        print(f"Frames found: {len(staged_frames)}")
        for key, value in staged_frames.items():
            # store the count of unnamed columns in a variable
            unnamed_cols = value.columns.str.contains("^Unnamed").sum()
            # capture the number of rows and cols
            r, c = value.shape
            # print the rows and columns of the dataframe
            print(
                f"- {key}; rows:{r}; columns:{c}{'(' + str(unnamed_cols) + ' unnamed)' if unnamed_cols else ''}"
            )
            print(value.head(1))
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
def preview(path: Optional[str] = typer.Argument(None), verbose: bool = False):
    # root = find_root()
    # cwd = Path(os.getcwd())
    # if root == cwd:
    path = clean_none_path(path)
    ca_path = get_ca_path(path)
    tree = plant_tree(ca_path)
    if tree and verbose:
        ymls = tree.get_eel_yml_preview(diff=False)
    elif tree:
        ymls = tree.get_eel_yml_preview(diff=True)
    else:
        raise Exception("tree not loaded")
    yaml_str = yaml.dump_all(ymls, sort_keys=False, allow_unicode=True)
    write_yaml_str(yaml_str)
    # else:
    #     print("current path different than eel root")


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
    # if os.path.exists(
    #     "D:\\Sync\\test_data\\eel-wb-population\\targets\\excel_container.xlsx"
    # ):
    #     os.remove(
    #         "D:\\Sync\\test_data\\eel-wb-population\\targets\\excel_container.xlsx"
    #     )
    os.chdir("D:\\Sync\\test_data\\eel-wb-population\\sources-project")
    execute()
    # print(list(staged_frames.values())[0].dtypes)
