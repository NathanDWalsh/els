import logging
from collections.abc import Iterable
from typing import Optional, Union

import numpy as np
import pandas as pd

import els.config as ec
import els.core as el
from els.io.base import ContainerReaderABC, ContainerWriterABC
from els.io.csv import CSVContainer
from els.io.fwf import FWFContainer
from els.io.pd import DFContainer
from els.io.pdf import PDFContainer
from els.io.sql import SQLContainer
from els.io.xl import XLContainer
from els.io.xml import XMLContainer


def get_container_class(
    frame: ec.Frame,
) -> type[Union[ContainerWriterABC, ContainerReaderABC]]:
    if frame.type == ".csv":
        return CSVContainer
    elif frame.type_is_excel:
        return XLContainer
    elif frame.type_is_db:
        return SQLContainer
    elif frame.type == "dict":
        return DFContainer
    elif frame.type == ".fwf":
        return FWFContainer
    elif frame.type == ".xml":
        return XMLContainer
    elif frame.type == ".pdf":
        return PDFContainer
    else:
        raise Exception(
            f"unknown {[type(frame), frame.model_dump(exclude_none=True)]} type: {frame.type}"
        )


def push_frame(
    df: pd.DataFrame,
    target: ec.Target,
    build: bool = False,
) -> bool:
    if not target or not target.type or not target.url:
        print("no target defined, printing first 100 rows:")
        print(df.head(100))
    else:
        container_class = get_container_class(target)
        df_container = el.fetch_df_container(
            container_class,
            url=target.url,
            replace=target.replace_container,
        )
        assert isinstance(target.table, str)
        df_table = df_container.fetch_child(
            df_name=target.table,
            df=df,
        )
        df_table.set_df(
            df=df,
            if_exists=target.if_table_exists,
            build=build,
            kwargs_push=target.kwargs_push,
        )
    return True


# TODO: add tests for this:
def config_frames_consistent(config: ec.Config) -> bool:
    target, source, transform = get_configs(config)

    # THIS LOGIC MAY NEED TO BE RESSURECTED
    # IT IS IGNORING IDENTITY/PRIMARY KEY FIELDS IN DATABASE,
    # ASSUMING THEY SHOULD NOT BE WRITTEN TO AND WILL NOT ALIGN WITH SOURCE
    # ignore_cols = []
    # if add_cols:
    #     for k, v in add_cols.items():
    #         if v == ec.DynamicColumnValue.ROW_INDEX.value:
    #             ignore_cols.append(k)

    source_df = pull_frame(source, sample=True)
    source_df = apply_transforms(source_df, transform, mark_as_executed=False)
    target_df = pull_frame(target, sample=True)
    return data_frames_consistent(source_df, target_df)


def apply_transforms(
    df: pd.DataFrame,
    transforms: Iterable[ec.TransformType],
    mark_as_executed: bool = True,
) -> pd.DataFrame:
    if not transforms == [None]:
        for transform in transforms:
            if not transform.executed:
                df = transform(
                    df,
                    mark_as_executed=mark_as_executed,
                )
    return df


def data_frames_consistent(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    ignore_cols: Optional[Iterable[str]] = None,
) -> bool:
    res = True
    if ignore_cols is None:
        ignore_cols = set()
    else:
        ignore_cols = set(ignore_cols)

    # Compare the column names and types
    source_cols = set(df1.columns.tolist()) - ignore_cols
    target_cols = set(df2.columns.tolist()) - ignore_cols

    if source_cols != target_cols:
        in_source = source_cols - target_cols
        in_target = target_cols - source_cols
        if in_source:
            logging.info("source has more columns:" + str(in_source))
        if in_target:
            logging.info("target has more columns:" + str(in_target))
        res = False
    else:
        for col in source_cols:
            # if nulls are returned from sql and object type is set in df
            if df2[col].dtype != "object" and df1[col].dtype != df2[col].dtype:
                logging.info(
                    f"{col} has a different data type source "
                    f"{df1[col].dtype} target {df2[col].dtype}"
                )
                res = False

    return res  # Table exists and has the same field names and types


def pull_frame(
    frame: Union[ec.Source, ec.Target],
    sample: bool = False,
) -> pd.DataFrame:
    container_class = get_container_class(frame)
    df_container = el.fetch_df_container(
        container_class=container_class,
        url=frame.url,  # type: ignore
    )
    assert isinstance(frame.table, str)
    df_table = df_container[frame.table]
    df = df_table.read(
        kwargs=frame.kwargs_pull,
        sample=sample,
    )

    if frame and hasattr(frame, "dtype") and frame.dtype:
        # assert df is not None
        for k, v in frame.dtype.items():
            if v == "date" and not isinstance(type(df[k]), np.dtypes.DateTime64DType):
                df[k] = pd.to_datetime(df[k])
    return pd.DataFrame(df)


def get_configs(
    config: ec.Config,
) -> tuple[
    ec.Target,
    ec.Source,
    list[ec.TransformType],
]:
    target = config.target
    source = config.source
    transform = config.transform_list

    return target, source, transform


def ingest(config: ec.Config) -> bool:
    target, source, transform = get_configs(config)
    consistent = config_frames_consistent(config)
    print("ingest")
    if not target or not target.table or consistent or target.consistency == "ignore":
        source_df = pull_frame(source, sample=False)
        source_df = apply_transforms(source_df, transform)
        print(source_df)
        return push_frame(source_df, target)
    else:
        raise Exception(f"{target.table}: Inconsistent, not saved.")


def table_exists(target: ec.Target) -> bool:
    assert target.url
    if target.type in (".csv", ".tsv"):
        return target.file_exists
    elif (
        target.type_is_db
        or (target.type_is_excel and target.file_exists)
        or target.type in ("dict")
    ):
        container_class = get_container_class(target)
        container = el.fetch_df_container(container_class, target.url)
        assert isinstance(target.table, str)
        return target.table in container
    else:
        return False


def requires_build_action(
    target: ec.Target,
) -> bool:
    if target.url_scheme == "file" and target.if_exists == "replace_file":
        return True
    elif target.type_is_db and target.if_exists == "replace_database":
        return True
    elif not table_exists(target) or target.if_exists == "replace":
        return True
    else:
        return False


def build(config: ec.Config) -> bool:
    target, source, transform = get_configs(config)
    print("build")
    if requires_build_action(target):
        sample = False if config.transforms_vary_target_columns else True
        df = pull_frame(source, sample=sample)
        df = apply_transforms(df, transform, mark_as_executed=False)
        return push_frame(df, target, build=True)
    else:
        return True
