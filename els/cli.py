from __future__ import annotations

import importlib.metadata
import io
import logging
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union

import pandas as pd
import ruamel.yaml as yaml
import typer

import els.core as el
import els.io.base as eio
from els.config import Config
from els.path import (
    CONFIG_FILE_EXT,
    ConfigPath,
    NodeType,
    get_root_config_name,
    get_root_inheritance,
    plant_memory_tree,
    plant_tree,
)

if TYPE_CHECKING:
    import els.flow as ef

# from pygments import highlight
# from pygments.lexers import YamlLexer
# from pygments.formatters import TerminalFormatter


app = typer.Typer()


def start_logging() -> None:
    logging.basicConfig(level=logging.ERROR, format="%(relativeCreated)d - %(message)s")
    # logging.disable(logging.CRITICAL)
    logging.info("Getting Started")


def get_ca_path(path: Optional[str] = None) -> Path:
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


class TaskFlow:
    def __init__(
        self,
        config_like: Optional[Union[str, Config]] = None,
        force_pandas_target: bool = False,
        nrows: Optional[int] = None,
    ):
        self.config_like = config_like
        self.force_pandas_target = force_pandas_target
        self.nrows = nrows
        self.taskflow = self.build()

    def __enter__(self) -> TaskFlow:
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.cleanup()

    def build(self) -> ef.ElsFlow:
        start_logging()
        if isinstance(self.config_like, str):
            ca_path = get_ca_path(self.config_like)
            tree = plant_tree(ca_path)
        else:
            ca_path = get_ca_path("./__dynamic__.els.yml")
            assert self.config_like
            tree = plant_memory_tree(ca_path, self.config_like)

        if self.force_pandas_target:
            tree.set_pandas_target(force=True)
        else:
            tree.set_pandas_target(force=False)

        if self.nrows:
            tree.set_nrows(self.nrows)
        if tree:
            return tree.get_ingest_taskflow()
        else:
            raise Exception("TaskFlow not built")

    def cleanup(self) -> None:
        for container in el.df_containers.values():
            if isinstance(container, eio.ContainerWriterABC):
                container.write()
            container.close()
        el.df_containers.clear()

        # just in case files still open
        for file in el.io_files.values():
            file.close()
        el.io_files.clear()

    def display_tree(self) -> None:
        self.taskflow.display_tree()

    def execute(self) -> None:
        self.taskflow.execute()


# remove node and assign children grandparent
def remove_node_and_adopt_orphans(node: ConfigPath) -> None:
    parent = node.parent
    if parent is not None:
        for child in node.children:
            # retain existing config chain
            child.parent = parent
        node.parent = None  # Detach the node from the tree


# Remove implicit config node
def remove_virtual_nodes(config_tree: ConfigPath) -> ConfigPath:
    if config_tree.node_type == NodeType.CONFIG_VIRTUAL:
        return config_tree.children[0]
    # Iterate through the tree in reverse order
    for node in config_tree.then_descendants:
        # If the node is virtual
        if node.node_type == NodeType.CONFIG_VIRTUAL:
            # Remove the node and reassign its children to its parent
            remove_node_and_adopt_orphans(node)
    return config_tree


@app.command()
def tree(
    path: Optional[str] = typer.Argument(None),
    keep_virtual: bool = False,
) -> None:
    if isinstance(path, Config):
        tree = plant_memory_tree(Path("./__dynamic__.els.yml"), path)
    else:
        path = clean_none_path(path)
        ca_path = get_ca_path(path)
        tree = plant_tree(ca_path)
    tree.set_pandas_target(force=False)
    # tree = remove_redundant_nodes(tree)
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
) -> None:
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
                write_yaml_str(yaml_str)


def organize_yaml_files_for_output(
    yamls: list[dict[str, Any]],
    table_filter: Optional[list[str]] = None,
) -> dict[str, list[dict[str, Any]]]:
    current_path = None
    res: dict[str, list[dict[str, Any]]] = dict()
    previous_path = ""
    for yml in yamls:
        if "config_path" in yml:
            current_path = yml.pop("config_path")
            if current_path != previous_path:
                res[current_path] = []
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
            assert current_path
            res[current_path].append(yml)
    return res


def process_ymls(
    ymls: list[dict[str, Any]],
    overwrite: bool = False,
) -> None:
    current_path = None
    for yml_dict in ymls:
        # Check if 'config_path' is present
        if "config_path" in yml_dict:
            current_path = yml_dict["config_path"]
            # Prepare the dict for serialization by removing 'config_path'
            yml_dict.pop("config_path")

        serialized_yaml: str = yaml.dump(yml_dict, default_flow_style=False)
        assert isinstance(serialized_yaml, str)
        if overwrite and current_path:
            # Append to the file if it's meant for multiple documents
            mode = "a" if "---" in serialized_yaml else "w"
            with open(current_path, mode) as file:
                file.write(serialized_yaml)
                file.write("\n---\n")  # Separate documents within the same file
        else:
            print(serialized_yaml)


@app.command()
def flow(
    path: Optional[str] = typer.Argument(None),
) -> None:
    path = clean_none_path(path)
    with TaskFlow(path) as taskflow:
        taskflow.display_tree()

    logging.info("Fin")


def clean_none_path(
    path: Optional[Union[str, typer.models.ArgumentInfo]],
) -> Optional[str]:
    if (
        (path is None)
        or isinstance(path, typer.models.ArgumentInfo)
        and path.default is None
    ):
        return None
    assert isinstance(path, str)
    return path


@app.command()
def preview(
    path: Optional[str] = typer.Argument(None),
    nrows: int = 4,
    transpose: bool = False,
) -> None:
    path = clean_none_path(path)
    with TaskFlow(
        path,
        force_pandas_target=True,
        nrows=nrows,
    ) as taskflow:
        taskflow.execute()

    if el.default_target:
        pd.set_option("display.show_dimensions", False)
        pd.set_option("display.max_columns", 4)
        pd.set_option("display.width", None)
        pd.set_option("display.max_colwidth", 18)
        pd.set_option("display.max_rows", None)

        for name, df in el.default_target.items():
            r, c = df.shape
            print(f"{name} [{r} rows x {c} columns]:")
            # df.index.name = " "
            if transpose:
                print(df.T)
            else:
                print(df.head(nrows))
            print()


@app.command()
def execute(path: Optional[str] = typer.Argument(None)) -> None:
    if isinstance(path, str):
        path = clean_none_path(path)

    with TaskFlow(path) as taskflow:
        taskflow.execute()

    if el.default_target and not isinstance(path, Config):
        typer.echo("No target specified; printing the first five rows of each table:\n")
        pd.set_option("display.max_rows", 5)

        for name, df in el.default_target.items():
            typer.echo(f"- {name}")
            df.index.name = " "
            typer.echo(df)

    logging.info("Fin")


def write_yaml_str(yaml_str: str) -> None:
    # if sys.stdout.isatty() and 1 == 2:
    #     colored_yaml = highlight(yaml_str, YamlLexer(), TerminalFormatter())
    #     sys.stdout.write(colored_yaml)
    # else:
    sys.stdout.write(yaml_str)


# def concat_enum_values(enum_class: Type[Enum]) -> str:
#     # Use a list comprehension to get the value of each enum member
#     values = [member.value for member in enum_class]
#     values = sorted(values)
#     # Concatenate the values into a single string
#     concatenated_values = ",".join(values)
#     return concatenated_values


@app.command()
def test() -> None:
    yml = yaml.YAML()
    contents = {"target": {"url": "../target/*.csv", "if_exists": "fail"}}
    yml_stream = io.StringIO()
    yml.dump(contents, yml_stream)
    yml_obj = yml.load(yml_stream.getvalue())
    # comment = concat_enum_values(TargetIfExistsValue)
    # yml_obj["target"].yaml_add_eol_comment(comment, key="if_exists")
    yml_stream = io.StringIO()
    yml.dump(yml_obj, yml_stream)
    print(yml_stream.getvalue())
    # yml = ryaml.load(yml_str)
    # config_default = Config()
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
) -> None:
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

    assert name
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
        # comment = concat_enum_values(TargetIfExistsValue)
        # yml_obj["target"].yaml_add_eol_comment(comment, key="if_exists")
        # yml_stream = io.StringIO()
        # yml.dump(yml_obj, yml_stream)

        # Serialize and write the contents to the file
        with open(config_file_path, "w") as file:
            yml.dump(yml_obj, file)

        typer.echo("Creating project config file:")
        typer.echo(f" ./{project_path.name}/config/{get_root_config_name()}")

    typer.echo("Done!")


@app.command()
def root() -> None:
    root = get_root_inheritance()
    print(root[-1])


@app.command()
def version() -> None:
    print(importlib.metadata.version("elspec"))


def main() -> None:
    start_logging()
    app()
