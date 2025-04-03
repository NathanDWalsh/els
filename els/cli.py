import importlib.metadata
import io
import logging
import os
import sys
from pathlib import Path
from typing import Optional, Union

import pandas as pd
import ruamel.yaml as yaml
import typer
from anytree import PreOrderIter

import els.core as el
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


class TaskFlow:
    def __init__(
        self,
        config_like: Union[str, Config] = None,
        force_pandas_target: bool = False,
        nrows: Optional[bool] = None,
    ):
        self.config_like = config_like
        self.force_pandas_target = force_pandas_target
        self.nrows = nrows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cleanup()

    def cleanup(self):
        for dbconn in el.open_sqls.values():
            dbconn.write()
            dbconn.close()
        el.open_sqls.clear()

        for excel_file in el.open_workbooks.values():
            excel_file.write()
            excel_file.close()
        el.open_workbooks.clear()

        for _dict in el.open_dicts.values():
            _dict.write()
        el.open_dicts.clear()

        # just in case files still open
        for file in el.open_files.values():
            file.close()
        el.open_files.clear()

        for eng in el.open_sa_engs.values():
            eng.dispose()
        el.open_sa_engs.clear()

    def execute(self):
        start_logging()
        if isinstance(self.config_like, str):
            ca_path = get_ca_path(self.config_like)
            tree = plant_tree(ca_path)
        else:
            ca_path = get_ca_path("./__dynamic__.els.yml")
            tree = plant_memory_tree(ca_path, self.config_like)

        if self.force_pandas_target:
            tree.set_pandas_target(force=True)
        else:
            tree.set_pandas_target(force=False)

        if self.nrows:
            tree.set_nrows(self.nrows)
        if tree:
            taskflow = tree.get_ingest_taskflow()
            taskflow.execute()
        else:
            raise Exception("TaskFlow not built")


# remove node and assign children grandparent
def remove_node_and_adopt_orphans(node: ConfigPath):
    parent = node.parent
    if parent is not None:
        for child in node.children:
            # retain existing config chain
            child.config_local = child.config
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
            remove_node_and_adopt_orphans(node)
    return tree


@app.command()
def tree(path: Optional[str] = typer.Argument(None), keep_virtual: bool = False):
    if isinstance(path, Config):
        tree = plant_memory_tree(Path("./__dynamic__.els.yml"), path)
    else:
        path = clean_none_path(path)
        ca_path = get_ca_path(path)
        tree = plant_tree(ca_path)
    tree.set_pandas_target(force=False)
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
                write_yaml_str(yaml_str)


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
    with TaskFlow(path) as taskflow:
        taskflow.display_tree()

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
    with TaskFlow(path, force_pandas_target=True, nrows=nrows) as taskflow:
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
def execute(path: Optional[str] = typer.Argument(None)):
    if isinstance(path, str):
        path = clean_none_path(path)

    with TaskFlow(path) as taskflow:
        taskflow.execute()

    if el.default_target and not isinstance(path, Config):
        print("\nNo target specified, sources saved to dataframes.\n\nTable summary:")

        print()
        print("Printing the first five rows of each DataFrame below:\n")

        pd.set_option("display.max_rows", 5)

        for name, df in el.default_target.items():
            print(f"{name}:")
            df.index.name = " "
            print(df)
            print()

    logging.info("Fin")


def write_yaml_str(yaml_str):
    # TODO, check the colors issues with pwsh
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
def test():
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
def root():
    root = get_root_inheritance()
    print(root[-1])


@app.command()
def version():
    print(importlib.metadata.version("elspec"))


def main():
    start_logging()
    app()
