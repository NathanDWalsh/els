import io
import os
from typing import Optional, Union

import pandas as pd

import els.io.base as eio

default_target: dict[str, pd.DataFrame] = {}
url_dicts: dict[str, dict[str, pd.DataFrame]] = {}
io_files: dict[str, io.BytesIO] = {}
df_containers: dict[str, eio.ContainerProtocol] = {}


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


def fetch_df_container(  # type: ignore
    container_class: type[eio.ContainerProtocol],
    url: Optional[str],
    replace: bool = False,
) -> Union[eio.ContainerReaderABC, eio.ContainerWriterABC]:  # type: ignore
    if isinstance(url, str):
        if url in df_containers:
            res = df_containers[url]
        else:
            res = container_class(
                url=url,
                replace=replace,
            )
    else:
        raise Exception(f"Cannot fetch {type(container_class)} from: {url}")
    df_containers[url] = res
    return res  # type: ignore


def fetch_file_io(
    url: Optional[str],
    replace: bool = False,
):
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
            # res = io.StringIO(file.read())
    else:
        res = io.BytesIO()
        # res = io.StringIO()
    io_files[url] = res
    return res
