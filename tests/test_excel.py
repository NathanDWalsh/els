import os

import numpy as np
import pandas as pd

from els.cli import execute
from els.config import Config, Source, Target

from . import helpers as th


def push_old(tmp_path, dfs, target=None):
    os.chdir(tmp_path)
    th.outbound.clear()
    for name, df in dfs.items():
        th.outbound[name] = df
    el_config_o = Config()
    el_config_o.source.df_dict = th.outbound
    el_config_o.target.url = f"{tmp_path.name}.xlsx"
    if target:
        el_config_o.target = Target.model_validate(
            el_config_o.target.model_dump(exclude_none=True) | target
        )
    execute(el_config_o)


def push(tmp_path, target=None):
    os.chdir(tmp_path)
    el_config_o = Config()
    el_config_o.source.df_dict = th.outbound

    el_config_o.target.url = f"{tmp_path.name}.xlsx"
    if target:
        el_config_o.target = Target.model_validate(
            el_config_o.target.model_dump(exclude_none=True) | target
        )
    execute(el_config_o)


def pull(tmp_path, source=None):
    th.inbound.clear()
    el_config_i = Config()
    el_config_i.source.url = f"{tmp_path.name}.xlsx"
    el_config_i.target.df_dict = th.inbound
    if source:
        el_config_i.source = Source.model_validate(
            el_config_i.source.model_dump(exclude_none=True) | source
        )
    execute(el_config_i)


def test_xl_single(tmp_path):
    th.single([push, pull], tmp_path)


def test_xl_double_together(tmp_path):
    th.double_together([push, pull], tmp_path)


def test_xl_append_together(tmp_path):
    th.append_together([push, pull], tmp_path)


def test_xl_skiprows(tmp_path):
    th.single(
        [
            (push, {"target": {"to_excel": {"startrow": 2}}}),
            (pull, {"source": {"read_excel": {"skiprows": 2}}}),
        ],
        tmp_path,
    )

    pull(tmp_path)
    df1 = th.inbound["df"]
    assert len(df1) == 5


def test_sheet_skipfooter(tmp_path):
    df0 = pd.DataFrame({"a": [1, 2, 3]})
    df0f = pd.DataFrame({"a": [1, 2, 3, None, None, "Footer"]})

    push_old(tmp_path, {"df1": df0f})

    pull(tmp_path)
    df1 = th.inbound["df1"]
    assert len(df1) == 6
    th.assert_dfs_equal(df0f, df1)

    pull(tmp_path, {"read_excel": {"skipfooter": 3}})
    df1 = th.inbound["df1"]
    th.assert_dfs_equal(df0, df1)


def test_append_together(tmp_path):
    th.append_together([push, pull], tmp_path=tmp_path)


def test_append_mixed(tmp_path):
    th.append_mixed([push, pull], tmp_path=tmp_path)


def test_truncate_single(tmp_path):
    th.truncate_single(push, pull, tmp_path)


# def test_truncate_double(tmp_path):
#     th.truncate_double(push, pull, tmp_path)


def test_append_plus(tmp_path):
    # th.append_plus([push, pull], tmp_path=tmp_path)
    df0a = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [4, 5, 6],
        }
    )
    df0b = pd.DataFrame(
        {
            "b": [40, 50, 60],
            "a": [10, 20, 30],
            "c": [70, 80, 90],
        }
    )
    df0 = pd.DataFrame(
        {
            "a": [1, 2, 3, 10, 20, 30],
            "b": [4, 5, 6, 40, 50, 60],
        }
    )

    push_old(tmp_path, {"df": df0a})
    push_old(tmp_path, {"df": df0b}, {"if_exists": "append", "consistency": "ignore"})

    pull(tmp_path)
    df1 = th.inbound["df"]
    th.assert_dfs_equal(df0, df1)


def test_truncate_plus(tmp_path):
    df0a = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [4, 5, 6],
        }
    )
    df0b = pd.DataFrame(
        {
            "b": [40, 50, 60],
            "a": [10, 20, 30],
            "c": [70, 80, 90],
        }
    )
    df1 = pd.DataFrame(
        {
            "a": [10, 20, 30],
            "b": [40, 50, 60],
        }
    )

    push_old(tmp_path, {"df": df0a})
    push_old(tmp_path, {"df": df0b}, {"if_exists": "truncate", "consistency": "ignore"})

    pull(tmp_path)
    df0 = th.inbound["df"]
    th.assert_dfs_equal(df0, df1)


def test_append_minus(tmp_path):
    # adding Nones to coerce datatypes to floats
    df0a = pd.DataFrame(
        {
            "a": [1, 2, None, 3],
            "b": [4, 5, None, 6],
        }
    )
    df0b = pd.DataFrame(
        {
            "b": [40, 50, 60],
        }
    )
    df1 = pd.DataFrame(
        {
            "a": [1, 2, None, 3, None, None, None],
            "b": [4, 5, None, 6, 40, 50, 60],
        }
    )

    push_old(tmp_path, {"df": df0a})
    push_old(tmp_path, {"df": df0b}, {"if_exists": "append", "consistency": "ignore"})

    pull(tmp_path)
    df0 = th.inbound["df"]
    th.assert_dfs_equal(df0, df1)


def test_truncate_minus(tmp_path):
    # adding Nones to coerce datatypes to floats
    df0a = pd.DataFrame(
        {
            "a": [1, 2, None, 3],
            "b": [4, 5, None, 6],
        }
    )
    df0b = pd.DataFrame(
        {
            "b": [40, 50, 60],
        }
    )
    df1 = pd.DataFrame(
        {
            "a": [np.nan, np.nan, np.nan],
            "b": [40, 50, 60],
        }
    )

    push_old(tmp_path, {"df": df0a})
    push_old(tmp_path, {"df": df0b}, {"if_exists": "truncate", "consistency": "ignore"})

    pull(tmp_path)
    df0 = th.inbound["df"]
    th.assert_dfs_equal(df0, df1)


def test_replace_sheet(tmp_path):
    df0a = pd.DataFrame({"a": [1, 2, 3]})
    df0b = pd.DataFrame({"b": [4, 5, 6]})
    df0c = pd.DataFrame({"c": [7, 8, 9]})

    push_old(tmp_path, {"df0a": df0a, "df0b": df0b})

    pull(tmp_path)
    df1a = th.inbound["df0a"]
    df1b = th.inbound["df0b"]
    th.assert_dfs_equal(df0a, df1a)
    th.assert_dfs_equal(df0b, df1b)

    push_old(tmp_path, {"df0b": df0c}, {"if_exists": "replace"})

    pull(tmp_path)
    assert (len(th.inbound)) == 2
    df1a = th.inbound["df0a"]
    df1b = th.inbound["df0b"]
    th.assert_dfs_equal(df0a, df1a)
    th.assert_dfs_equal(df0c, df1b)


def test_replace_file(tmp_path):
    df0a = pd.DataFrame({"a": [1, 2, 3]})
    df0b = pd.DataFrame({"b": [4, 5, 6]})

    push_old(tmp_path, {"df0": df0a})
    push_old(tmp_path, {"df1": df0b}, {"if_exists": "replace_file"})

    pull(tmp_path)
    df0a = th.inbound["df1"]
    assert len(th.inbound) == 1
    th.assert_dfs_equal(df0b, df0a)
