import io
import os

import pandas as pd

import els.io.base as eio

default_target: dict[str, pd.DataFrame] = {}
url_dicts: dict[str, dict[str, pd.DataFrame]] = {}
io_files: dict[str, io.BytesIO] = {}
df_containers: dict[str, eio.ContainerWriterABC] = {}


def fetch_df_dict(
    url: str,
    replace: bool = False,
):
    res = url_dicts[url]
    if replace:
        res.clear()
    return res


def urlize_dict(df_dict: dict[str, pd.DataFrame]):
    res = f"dict://{id(df_dict)}"
    if res not in url_dicts:
        url_dicts[res] = df_dict
    return res


def fetch_df_container(
    container_class: type[eio.ContainerWriterABC],
    url: str,
    replace: bool = False,
) -> eio.ContainerWriterABC:
    if isinstance(url, str):
        if url in df_containers:
            res = df_containers[url]
        else:
            res = container_class(
                url=url,
                replace=replace,
            )  # type: ignore
    else:
        raise Exception(f"Cannot fetch {type(container_class)} from: {url}")
    df_containers[url] = res
    return res


def fetch_file_io(url: str, replace: bool = False):
    if url is None:
        raise Exception("Cannot fetch None url")
    elif url in io_files:
        res = io_files[url]
    # only allows replacing once:
    elif replace:
        res = io.BytesIO()
    # chck file exists:
    elif os.path.isfile(url):
        with open(url, "rb") as file:
            res = io.BytesIO(file.read())
    else:
        res = io.BytesIO()
    io_files[url] = res
    return res


def listify(v):
    return v if isinstance(v, (list, tuple)) else [v]
