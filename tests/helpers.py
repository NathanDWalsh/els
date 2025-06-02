import os
import sys
from typing import Literal, Union

import pandas as pd
import yaml

import els.cli as ei
import els.config as ec
from els._typing import listify

TestMedium_ = Literal[
    "pandas",
    "excel",
    "sqlite",
    "duckdb",
    "mssql",
]
if sys.version_info >= (3, 10):
    from typing import TypeAlias

    TestMedium: TypeAlias = TestMedium_
else:
    TestMedium = TestMedium_


def assert_dfs_equal(df0: pd.DataFrame, df1: pd.DataFrame):
    assert len(df0) == len(df1)
    assert len(df0.columns) == len(df1.columns)
    if not df0.dtypes.equals(df1.dtypes):
        raise Exception(f"types not equal: {[df0.dtypes], [df1.dtypes]}")
    assert df0.dtypes.equals(df1.dtypes)
    assert df0.columns.equals(df1.columns)
    assert df0.index.equals(df1.index)
    assert df0.equals(df1)


def assert_expected(expected, actual):
    assert expected is not actual
    assert len(expected) > 0
    for k in expected.keys():
        if k not in actual:
            raise Exception([expected, actual])
        assert k in actual
        if isinstance(expected, dict):
            assert_dfs_equal(expected[k], actual[k])
        else:
            assert_dfs_equal(expected[[k]], actual[[k]])


def to_call_list(for_calling):
    for_calling = listify(for_calling)
    res = []
    for i in for_calling:
        if isinstance(i, tuple):
            res.append(i)
        elif i:
            res.append((i, {}))
    return res


def parse_func_and_kwargs(for_calling, global_kwargs: dict):
    for_calling = to_call_list(for_calling)

    res = []

    push_kwargs = {}
    pull_kwargs = {}
    for k, v in global_kwargs.items():
        if k == "target":
            push_kwargs[k] = v
        elif v == "source":
            pull_kwargs[k] = v
        else:
            push_kwargs[k] = v
            pull_kwargs[k] = v

    for func in for_calling:
        if func[0].__name__ == "push":
            res.append((func[0], func[1] | push_kwargs))
        elif func[0].__name__ == "pull":
            res.append((func[0], func[1] | pull_kwargs))
        else:
            raise Exception(f"function not supported {func}")

    return res


def call_io_funcs(for_calling, **kwargs):
    for_calling = parse_func_and_kwargs(for_calling, kwargs)

    for func in for_calling:
        func[0](**func[1])


def filename_from_dir(extension=None):
    dirpath = os.getcwd()
    foldername = os.path.basename(dirpath)

    return f"{foldername}{('.' + extension) if extension else ''}"


def config_dump(config: ec.Config, file_names: Union[str, list[str]]):
    file_names = listify(file_names)
    for file_name in file_names:
        yaml.dump(
            config.model_dump(
                mode="json",
                exclude_none=True,
            ),
            open(file_name, "w"),
            sort_keys=False,
            allow_unicode=True,
        )


def config_execute(config: ec.Config, as_yaml_file_name=None):
    if as_yaml_file_name:
        config_dump(config, as_yaml_file_name)
        execute = as_yaml_file_name
    else:
        execute = config

    ei.tree(execute)
    # ei.flow(execute)
    ei.execute(execute)
