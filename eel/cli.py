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


@app.command()
def tree():
    tree = create_tree()
    tree.root.display_tree()
    logging.info("Fin")


@app.command()
def flow():
    tree = create_tree()
    taskflow = tree.root.get_ingest_taskflow()
    print(taskflow.to_tuple)
    # taskflow.execute()
    logging.info("Fin")


# @app.command()
# def prev():
#     tree = create_tree()
#     tree.root.save_eel_yml_preview()


def main():
    start_logging()
    app()


if __name__ == "__main__":
    main()
