import typer
import logging

import eel.tree as et

app = typer.Typer()


def start_logging():
    logging.basicConfig(level=logging.INFO, format="%(relativeCreated)d - %(message)s")
    logging.info("Getting Started")


def create_tree() -> et.EelTree:
    tree = et.EelTree()
    logging.info("Tree Created")
    return tree


def get_taskflow():
    tree = create_tree()
    taskflow = tree.root.get_ingest_taskflow()
    return taskflow


@app.command()
def tree():
    tree = create_tree()
    tree.root.display_tree()
    logging.info("Fin")


@app.command()
def flow():
    taskflow = get_taskflow()
    taskflow.display_tree()
    # taskflow.execute()
    logging.info("Fin")


@app.command()
def execute():
    taskflow = get_taskflow()
    taskflow.execute()
    logging.info("Fin")


# @app.command()
# def preview():
#     tree = create_tree()
#     tree.root.save_eel_yml_preview()


def main():
    start_logging()
    app()


if __name__ == "__main__":
    main()