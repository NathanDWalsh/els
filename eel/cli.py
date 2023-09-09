import typer
import logging
import yaml
from pygments import highlight
from pygments.lexers import YamlLexer
from pygments.formatters import TerminalFormatter
import sys
import os

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
    tree = plant_tree()
    ymls = tree.get_eel_yml_preview()
    yaml_str = yaml.dump_all(ymls, sort_keys=False, allow_unicode=True)
    if sys.stdout.isatty():
        colored_yaml = highlight(yaml_str, YamlLexer(), TerminalFormatter())
        sys.stdout.write(colored_yaml)
    else:
        sys.stdout.write(yaml_str)


def main():
    start_logging()
    app()


if __name__ == "__main__":
    os.chdir("D:\\test_data")
    tree()
