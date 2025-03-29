import os

import pandas as pd

import els.config as ec

from . import helpers as th
from . import templates as tt


def flight_url():
    return th.filename_from_dir("xlsx")


def test_skiprows(tmp_path):
    os.chdir(tmp_path)
    outbound = dict(df=pd.DataFrame({"a": [1, 2, 3]}))
    expected = outbound
    config = ec.Config(
        target=ec.Target(
            to_excel=ec.ToExcel(startrow=2),
        )
    )
    tt.push(
        flight_url=flight_url,
        config=config,
        outbound=outbound,
    )
    inbound = tt.pull(
        flight_url=flight_url,
        config=ec.Config(
            source=ec.Source(
                read_excel=ec.ReadExcel(skiprows=2),
            ),
        ),
    )
    th.assert_expected(expected, inbound)

    inbound = tt.pull(flight_url=flight_url)
    df1 = inbound["df"]
    assert len(df1) == 5


def test_sheet_skipfooter(tmp_path):
    os.chdir(tmp_path)
    df0 = pd.DataFrame({"a": [1, 2, 3]})
    df0f = pd.DataFrame({"a": [1, 2, 3, None, None, "Footer"]})

    outbound = dict(df1=df0f)

    tt.push(flight_url=flight_url, outbound=outbound)

    inbound = tt.pull(flight_url=flight_url)
    df1 = inbound["df1"]
    assert len(df1) == 6
    th.assert_dfs_equal(df0f, df1)

    inbound = tt.pull(
        flight_url=flight_url,
        config=ec.Config(source=ec.Source(read_excel=ec.ReadExcel(skipfooter=3))),
    )
    df1 = inbound["df1"]
    th.assert_dfs_equal(df0, df1)


@tt.config_push
def replace_file():
    outbound = dict(
        df1=pd.DataFrame({"a": [1, 2, 3]}), df2=pd.DataFrame({"b": [4, 5, 6]})
    )
    expected = dict(df2=pd.DataFrame({"b": [4, 5, 6]}))

    config = [
        ec.Config(),
        ec.Config(
            source=ec.Source(table="df2"),
            target=ec.Target(if_exists="replace_file"),
        ),
    ]
    return outbound, expected, config


@tt.config_push
def multiindex_column():
    outbound = dict(
        dfx=pd.DataFrame(
            columns=pd.MultiIndex.from_product([["A", "B"], ["c", "d", "e"]]),
            data=[[1, 2, 3, 4, 5, 6]],
        )
    )
    expected = dict(
        dfx=pd.DataFrame(
            {"A_c": [1], "A_d": [2], "A_e": [3], "B_c": [4], "B_d": [5], "B_e": [6]}
        )
    )
    config = ec.Config()
    return outbound, expected, config
