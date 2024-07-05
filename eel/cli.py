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

# from eel.path import grow_branches
from eel.path import get_config_default
from eel.path import config_path_valid
from eel.path import CONFIG_FILE_EXT

from eel.execute import staged_frames

app = typer.Typer()


def start_logging():
    logging.basicConfig(level=logging.INFO, format="%(relativeCreated)d - %(message)s")
    # logging.disable(logging.CRITICAL)
    logging.info("Getting Started")


def find_root_paths(path: str = None) -> list[Union[CAPath, None]]:
    if path:
        path_arg = Path(path)
    else:
        path_arg = Path()

    paths_to_root = find_dirs_with_file(path_arg, get_root_config_name())
    # if paths_to_root:
    #     root = paths_to_root[-1]
    # else:
    #     root = None
    # if not paths_to_root:
    #     logging.info("eel root not found, using cwd")
    #     paths_to_root = [CAPath()]
    # if paths_to_root == [None]:
    #     logging.error("unknown error, root not found")
    #     return [None]
    # raise Exception("debug")
    return paths_to_root


def find_dirs_with_file(start_dir: Path, target_file: str) -> Union[list[CAPath], None]:
    dirs = []
    current_dir = start_dir.absolute()
    file_found = False
    while (
        current_dir != current_dir.parent
    ):  # This condition ensures we haven't reached the root
        dirs.append(current_dir)
        if (current_dir / target_file).exists():
            file_found = True
            break
        current_dir = current_dir.parent
    # Check and add the root directory if not already added
    if current_dir not in dirs and (current_dir / target_file).exists():
        dirs.append(current_dir)
        file_found = True
    # raise Exception()
    if file_found:
        # print(dirs)
        return dirs
    else:
        glob_pattern = "**/*" + target_file
        below = sorted(start_dir.glob(glob_pattern))
        if len(below) > 0:
            return [CAPath(below[0].parent.absolute())]
        else:
            logging.info(f"eel root not found, using {start_dir}")
            return [start_dir]


def plant_tree(path: CAPath) -> Optional[CAPath]:
    root_paths = list(reversed(find_root_paths(str(path))))
    root_path = Path(root_paths[0])
    if root_path.is_dir():
        os.chdir(root_path)
    else:
        os.chdir(root_path.parent)
    # print(root_paths)
    parent = None
    for index, path_ in enumerate(root_paths):
        if config_path_valid(path_):
            if index < len(root_paths) - 1:  # For all items except the last one
                ca_path = CAPath(path_, parent=parent)
                parent = ca_path
            # else:  # For the last item
            #     parent = grow_branches(
            #         path_, parent=parent
            #     )  # Assuming you want to call grow_branches for the last item
            else:
                ca_path = CAPath(path_, parent=parent, spawn_children=True)
        else:
            raise Exception("Invalid file in explicit path: " + str(path_))
    logging.info("Tree Created")
    root = parent.root if parent else ca_path
    if root.is_leaf and root.is_dir():
        logging.error("Root is an empty directory")
    return root


def get_ca_path(path: str = None) -> CAPath:
    if path:
        pl_path = Path(path)
        if pl_path.is_file() and not str(pl_path).endswith(CONFIG_FILE_EXT):
            ca_path = CAPath(path + CONFIG_FILE_EXT)
        else:
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
        print("No target specified, printing summary information on pandas dataframes:")
        print(f"Dataframe count: {len(staged_frames)}")

        # Calculate maximum width for each column
        key_width = (
            max(len(key) for key in staged_frames.keys()) + 2
        )  # Adding some padding
        rows_width = (
            max(len(str(value.shape[0])) for value in staged_frames.values()) + 2
        )
        cols_width = (
            max(len(str(value.shape[1])) for value in staged_frames.values()) + 2
        )
        unnamed_cols_width = (
            max(
                len(str(value.columns.str.contains("^Unnamed").sum()))
                for value in staged_frames.values()
            )
            + 2
        )

        # Ensure minimum width to fit headers
        key_width = max(key_width, len(""))
        rows_width = max(rows_width, len("Rows"))
        cols_width = max(cols_width, len("Columns"))
        unnamed_cols_width = max(unnamed_cols_width, len("Unnamed Columns"))

        # Print headers with dynamic width
        print(
            f"{'':{key_width}} {'Rows':{rows_width}} {'Columns':{cols_width}} {'Unnamed Columns':{unnamed_cols_width}}"
        )

        for key, value in staged_frames.items():
            # Store the count of unnamed columns in a variable
            unnamed_cols = value.columns.str.contains("^Unnamed").sum()
            # Capture the number of rows and cols
            r, c = value.shape
            # Print the rows and columns of the dataframe in four columns with dynamic width
            print(
                f"{key:{key_width}} {r:{rows_width}} {c:{cols_width}} {unnamed_cols if unnamed_cols else '':{unnamed_cols_width}}"
            )

        print(value.dtypes)

    logging.info("Fin")


def write_yaml_str(yaml_str):
    if sys.stdout.isatty() and 1 == 2:
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


def create_subfolder(project_path: Path, subfolder: str, silent: bool) -> None:
    if silent or typer.confirm(f"Do you want to create the {subfolder} folder?"):
        (project_path / subfolder).mkdir()
        typer.echo(f"{subfolder} folder created.")


@app.command()
def new(
    name: Optional[str] = typer.Argument(None),
    silent: bool = typer.Option(False, "--silent", "-s"),
):
    # Verify project creation in the current directory
    if not silent and not typer.confirm(
        "Verify project to be created in the current directory?"
    ):
        typer.echo("Project creation cancelled.")
        raise typer.Exit()

    # If no project name is provided and not silent, prompt for it
    if not name and not silent:
        name = typer.prompt("Enter the project directory name")
    elif not name:
        typer.echo("Project name is required in silent mode.")
        raise typer.Exit()

    project_path = Path(os.getcwd()) / name
    try:
        project_path.mkdir()
    except FileExistsError:
        typer.echo(f"The directory {name} already exists.")
        raise typer.Exit()

    # Create subfolders
    for subfolder in ["source", "target", "config"]:
        create_subfolder(project_path, subfolder, silent)

    typer.echo(f"Project {name} created successfully.")


@app.command()
def root():
    root = find_root_paths()
    print(root[-1])


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
    os.chdir("C:\\Users\\nwals\\eel-demo\\config")
    # os.chdir("D:\\Sync\\repos\\eel\\temp")
    # os.chdir("D:\\Sync\\test_data\\eel-wb-population\\excel_lite")
    # os.chdir("C:\\Users\\nwals\\eel-demo\\config\\excel")
    tree()
    # print(list(staged_frames.values())[0].dtypes)
