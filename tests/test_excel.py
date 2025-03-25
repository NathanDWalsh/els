import os

import pandas as pd
import yaml

import els.config as ec
from els.cli import execute, tree

from . import helpers as th
from . import templates as tt


def push(
    tmp_path,
    target=ec.Target(),
    source=ec.Source(),
    transform=None,
):
    os.chdir(tmp_path)
    config = ec.Config(target=target)

    config.source = source
    config.target = ec.Target.model_validate(target)
    config.transform = transform

    config.target.url = f"{tmp_path.name}.xlsx"
    config.source.df_dict = th.outbound

    test_els = "__.els.yml"
    yaml.dump(
        config.model_dump(exclude_none=True),
        open(test_els, "w"),
        sort_keys=False,
        allow_unicode=True,
    )

    yaml.dump(
        config.model_dump(exclude_none=True),
        open("__.bkp", "w"),
        sort_keys=False,
        allow_unicode=True,
    )

    tree(test_els)
    execute(test_els)

    # tree(config)
    # execute(config)


def pull(
    tmp_path,
    source=ec.Source(),
    transform=None,
):
    th.inbound.clear()
    config = ec.Config()

    config.source = ec.Source.model_validate(source)
    config.transform = transform

    config.source.url = f"{tmp_path.name}.xlsx"
    config.target.df_dict = th.inbound

    test_els = "__.els.yml"
    yaml.dump(
        config.model_dump(exclude_none=True),
        open(test_els, "w"),
        sort_keys=False,
        allow_unicode=True,
    )

    tree(test_els)
    execute(test_els)

    # tree(config)
    # execute(config)


def test_skiprows(tmp_path):
    tt.single(
        [
            (
                push,
                {
                    "target": ec.Target(to_excel=ec.ToExcel(startrow=2)).model_dump(
                        exclude_unset=True
                    )
                },
            ),
            (
                pull,
                {"source": ec.Source(read_excel=ec.ReadExcel(skiprows=2)).model_dump()},
            ),
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

    pull(tmp_path, {"read_excel": ec.ReadExcel(skipfooter=3).model_dump()})
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


def test_multiindex_column(tmp_path):
    th.outbound.clear()
    th.outbound["dfx"] = pd.DataFrame(
        columns=pd.MultiIndex.from_product([["A", "B"], ["c", "d", "e"]]),
        data=[[1, 2, 3, 4, 5, 6]],
    )
    push(tmp_path)
    pull(tmp_path)
    expected = {
        "dfx": pd.DataFrame(
            {"A_c": [1], "A_d": [2], "A_e": [3], "B_c": [4], "B_d": [5], "B_e": [6]}
        )
    }
    th.assert_expected(expected)
