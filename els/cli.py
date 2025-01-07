import importlib.metadata
import io
import logging
import os
import sys
from enum import Enum
from pathlib import Path
from typing import Optional, Type

import pandas as pd
import ruamel.yaml as yaml
import typer
from anytree import PreOrderIter

from els.config import TargetIfExistsValue
from els.execute import staged_frames
from els.path import (
    CONFIG_FILE_EXT,
    NodeType,
    get_root_config_name,
    get_root_inheritance,
    plant_tree,
)

# from pygments import highlight
# from pygments.lexers import YamlLexer
# from pygments.formatters import TerminalFormatter


app = typer.Typer()


def start_logging():
    logging.basicConfig(level=logging.ERROR, format="%(relativeCreated)d - %(message)s")
    # logging.disable(logging.CRITICAL)
    logging.info("Getting Started")


def get_ca_path(path: str = None) -> Path:
    if path:
        # may be related to "seemingly redundant" lines fix above
        pl_path = Path() / Path(path)
        if pl_path.is_file() and not str(pl_path).endswith(CONFIG_FILE_EXT):
            ca_path = Path(path + CONFIG_FILE_EXT)
        else:
            ca_path = Path(path)
    else:
        ca_path = Path()
    return ca_path


def get_taskflow(
    path: str = None, force_pandas_target: bool = False, nrows: Optional[bool] = None
):
    ca_path = get_ca_path(path)
    tree = plant_tree(ca_path)
    if force_pandas_target:
        tree.force_pandas_target()
    if nrows:
        tree.set_nrows(nrows)
    if tree:
        taskflow = tree.get_ingest_taskflow()
        return taskflow
    else:
        return None


# remove a node and reassign its children to its parent
def remove_node_and_reassign_children(node):
    parent = node.parent
    if parent is not None:
        for child in node.children:
            # retain existing config chain
            child._config = child.config
            child.parent = parent
        node.parent = None  # Detach the node from the tree


# Remove implicit config node
def remove_virtual_nodes(tree):
    if tree.node_type == NodeType.CONFIG_VIRTUAL:
        return tree.children[0]
    # Iterate through the tree in reverse order
    for node in PreOrderIter(tree):
        # If the node is virtual
        if node.node_type == NodeType.CONFIG_VIRTUAL:
            # Remove the node and reassign its children to its parent
            remove_node_and_reassign_children(node)
    return tree


@app.command()
def tree(path: Optional[str] = typer.Argument(None), keep_virtual: bool = False):
    path = clean_none_path(path)
    ca_path = get_ca_path(path)
    tree = plant_tree(ca_path)
    if not keep_virtual:
        tree = remove_virtual_nodes(tree)
    if tree:
        tree.display_tree()
    else:
        logging.error("tree not loaded")
    logging.info("Fin")


@app.command()
def generate(
    path: Optional[str] = typer.Argument(None),
    tables: Optional[str] = typer.Option(
        None, help="Comma-separated list of table names, optionally double-quoted"
    ),
    overwrite: bool = True,
    skip_root: bool = True,
):
    if tables:
        table_filter = [table.strip().strip('"') for table in tables.split(",")]
    else:
        table_filter = []
    verbose = False
    path = clean_none_path(path)
    ca_path = get_ca_path(path)
    tree = plant_tree(ca_path)

    if tree and verbose:
        ymls = tree.get_els_yml_preview(diff=False)
    elif tree:
        ymls = tree.get_els_yml_preview(diff=True)
    else:
        raise Exception("tree not loaded")
    yml_grouped = organize_yaml_files_for_output(ymls, table_filter)
    for file_name, yaml_file_content in yml_grouped.items():
        if not (skip_root and file_name.endswith(get_root_config_name())):
            yaml_stream = io.StringIO()
            yml = yaml.YAML()
            yml.dump_all(yaml_file_content, yaml_stream)
            yaml_str = yaml_stream.getvalue()
            if overwrite and yaml_str:
                with open(file_name, "w") as file:
                    file.write(yaml_str)
            elif yaml_str:
                # yaml_str = yaml.dump_all(ymls, sort_keys=False, allow_unicode=True)
                write_yaml_str(yaml_str)
    # else:
    #     print("current path different than els root")


def organize_yaml_files_for_output(
    yamls, table_filter: Optional[list[str]] = None
) -> dict[list[dict]]:
    current_path = None
    res = dict()
    previous_path = ""
    for yml in yamls:
        if "config_path" in yml:
            current_path = yml.pop("config_path")
            if current_path != previous_path:
                res[current_path] = []
            # res[current_path].append(yml)
            previous_path = current_path
        if (
            "source" in yml
            and "table" in yml["source"]
            and not ("target" in yml and "table" in yml["target"])
        ):
            if "target" not in yml:
                yml["target"] = dict()
            yml["target"]["table"] = yml["source"]["table"]
        if not table_filter or (
            table_filter
            and "target" in yml
            and "table" in yml["target"]
            and yml["target"]["table"] in table_filter
        ):
            res[current_path].append(yml)
    return res


def process_ymls(ymls, overwrite=False):
    current_path = None
    for yml_dict in ymls:
        # Check if 'config_path' is present
        if "config_path" in yml_dict:
            current_path = yml_dict["config_path"]
            # Prepare the dict for serialization by removing 'config_path'
            yml_dict.pop("config_path")

        # Serialize the YAML
        serialized_yaml = yaml.dump(yml_dict, default_flow_style=False)

        if overwrite and current_path:
            # Append to the file if it's meant for multiple documents
            mode = "a" if "---" in serialized_yaml else "w"
            with open(current_path, mode) as file:
                file.write(serialized_yaml)
                file.write("\n---\n")  # Separate documents within the same file
        else:
            print(serialized_yaml)


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
def preview(
    path: Optional[str] = typer.Argument(None),
    nrows: int = 4,
    transpose: bool = False,
):
    path = clean_none_path(path)
    # taskflow = get_taskflow(path, force_pandas_target=True, nrows=nrows)
    taskflow = get_taskflow(path, force_pandas_target=True)
    if taskflow:
        taskflow.execute()
        # print(pandas_end_points)
    else:
        logging.error("taskflow not loaded")
    if staged_frames:
        pd.set_option("display.show_dimensions", False)
        pd.set_option("display.max_columns", 4)
        pd.set_option("display.width", None)
        pd.set_option("display.max_colwidth", 18)
        pd.set_option("display.max_rows", None)

        # print()
        # print("Printing the first five rows of each DataFrame below:\n")

        for name, df in staged_frames.items():
            r, c = df.shape
            print(f"{name} [{r} rows x {c} columns]:")
            # df.index.name = " "
            if transpose:
                print(df.T)
            else:
                print(df.head(nrows))
            print()

        # print(value.dtypes)


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
        print("\nNo target specified, sources saved to dataframes.\n\nTable summary:")

        print()
        print("Printing the first five rows of each DataFrame below:\n")

        pd.set_option("display.max_rows", 5)

        for name, df in staged_frames.items():
            print(f"{name}:")
            df.index.name = " "
            print(df)
            print()

        # print(value.dtypes)

    logging.info("Fin")


def write_yaml_str(yaml_str):
    # TODO, check the colors issues with pwsh
    # if sys.stdout.isatty() and 1 == 2:
    #     colored_yaml = highlight(yaml_str, YamlLexer(), TerminalFormatter())
    #     sys.stdout.write(colored_yaml)
    # else:
    sys.stdout.write(yaml_str)


def concat_enum_values(enum_class: Type[Enum]) -> str:
    # Use a list comprehension to get the value of each enum member
    values = [member.value for member in enum_class]
    values = sorted(values)
    # Concatenate the values into a single string
    concatenated_values = ",".join(values)
    return concatenated_values


@app.command()
def test():
    yml = yaml.YAML()
    contents = {"target": {"url": "../target/*.csv", "if_exists": "fail"}}
    yml_stream = io.StringIO()
    yml.dump(contents, yml_stream)
    yml_obj = yml.load(yml_stream.getvalue())
    comment = concat_enum_values(TargetIfExistsValue)
    yml_obj["target"].yaml_add_eol_comment(comment, key="if_exists")
    yml_stream = io.StringIO()
    yml.dump(yml_obj, yml_stream)
    print(yml_stream.getvalue())
    # yml = ryaml.load(yml_str)
    # config_default = get_config_default()
    # yml = config_default.model_dump(exclude_none=True)
    # yaml_str = yaml.dump(yml, sort_keys=False, allow_unicode=True)
    # write_yaml_str(yaml_str)


def create_subfolder(project_path: Path, subfolder: str, silent: bool) -> None:
    if silent or typer.confirm(f"Do you want to create the {subfolder} folder?"):
        (project_path / subfolder).mkdir()
        typer.echo(f" ./{project_path.name}/{subfolder}/")


@app.command()
def new(
    name: Optional[str] = typer.Argument(None),
    yes: bool = typer.Option(False, "--yes", "-y"),
):
    # Verify project creation in the current directory
    if not yes and not typer.confirm(
        "Verify project to be created in the current directory?"
    ):
        typer.echo("Project creation cancelled.")
        raise typer.Exit()

    # If no project name is provided and not yes mode, prompt for it
    if not name and not yes:
        name = typer.prompt("Enter the project directory name")
    elif not name:
        typer.echo("Project name is required in yes mode.")
        raise typer.Exit()

    project_path = Path(os.getcwd()) / name
    try:
        project_path.mkdir()
        print("Creating directories:")
        print(f" ./{name}/")
    except FileExistsError:
        typer.echo(f"The directory {name} already exists.")
        raise typer.Exit()

    # Create subfolders
    for subfolder in ["config", "source", "target"]:
        create_subfolder(project_path, subfolder, yes)

    # Ensure the config folder exists
    config_folder_path = project_path / "config"
    if config_folder_path.exists():
        # Define the file path for __.els.yml
        config_file_path = config_folder_path / get_root_config_name()

        yml = yaml.YAML()
        contents = {"target": {"url": "../target/*.csv", "if_exists": "fail"}}
        yml_stream = io.StringIO()
        yml.dump(contents, yml_stream)
        yml_obj = yml.load(yml_stream.getvalue())
        comment = concat_enum_values(TargetIfExistsValue)
        yml_obj["target"].yaml_add_eol_comment(comment, key="if_exists")
        # yml_stream = io.StringIO()
        # yml.dump(yml_obj, yml_stream)

        # Serialize and write the contents to the file
        with open(config_file_path, "w") as file:
            yml.dump(yml_obj, file)

        typer.echo("Creating project config file:")
        typer.echo(f" ./{project_path.name}/config/{get_root_config_name()}")

    typer.echo("Done!")


@app.command()
def root():
    root = get_root_inheritance()
    print(root[-1])


@app.command()
def version():
    print(importlib.metadata.version("elspec"))


def main():
    start_logging()
    app()


if __name__ == "__main__":
    start_logging()
    # if os.path.exists(
    #     "D:\\Sync\\test_data\\els-wb-population\\targets\\excel_container.xlsx"
    # ):
    #     os.remove(
    #         "D:\\Sync\\test_data\\els-wb-population\\targets\\excel_container.xlsx"
    #     )
    os.chdir("C:\\Users\\nwals\\els-demo\\config")
    # os.chdir("D:\\Sync\\test_data\\els-wb-population\\excel_lite")
    # os.chdir("C:\\Users\\nwals\\els-demo\\config\\excel")
    tree()
    # print(list(staged_frames.values())[0].dtypes)
