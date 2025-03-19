import os

import pandas as pd

from els.cli import execute, tree
from els.config import Config, Source, Target

from . import helpers as th


def push(tmp_path, target=None):
    os.chdir(tmp_path)
    el_config_o = Config()
    el_config_o.source.df_dict = th.outbound

    el_config_o.target.url = f"{tmp_path.name}.xlsx"
    if target:
        el_config_o.target = Target.model_validate(
            el_config_o.target.model_dump(exclude_none=True) | target
        )

    tree(el_config_o)
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

    tree(el_config_i)
    execute(el_config_i)


def test_skiprows(tmp_path):
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

    th.outbound.clear()
    th.outbound["df1"] = df0f

    push(tmp_path)

    pull(tmp_path)
    df1 = th.inbound["df1"]
    assert len(df1) == 6
    th.assert_dfs_equal(df0f, df1)

    pull(tmp_path, {"read_excel": {"skipfooter": 3}})
    df1 = th.inbound["df1"]
    th.assert_dfs_equal(df0, df1)


def test_replace_file(tmp_path):
    df0a = pd.DataFrame({"a": [1, 2, 3]})
    df0b = pd.DataFrame({"b": [4, 5, 6]})

    th.outbound.clear()
    th.outbound["df0"] = df0a
    push(tmp_path)

    th.outbound.clear()
    th.outbound["df1"] = df0b
    push(tmp_path, {"if_exists": "replace_file"})

    pull(tmp_path)
    df0a = th.inbound["df1"]
    assert len(th.inbound) == 1
    th.assert_dfs_equal(df0b, df0a)
