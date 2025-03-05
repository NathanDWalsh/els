import os

import pandas as pd

from els.cli import execute
from els.config import Source, Target
from els.core import staged_frames
from els.path import get_config_default


def push(tmp_path, dfs, target=None):
    os.chdir(tmp_path)
    staged_frames.clear()
    for name, df in dfs.items():
        staged_frames[name] = df
    el_config_o = get_config_default()
    el_config_o.source.url = "pandas://"
    el_config_o.target.url = f"{tmp_path.name}.xlsx"
    if target:
        el_config_o.target = Target.model_validate(
            el_config_o.target.model_dump(exclude_none=True) | target
        )
    execute(el_config_o)


def pull(tmp_path, source=None):
    staged_frames.clear()
    el_config_i = get_config_default()
    el_config_i.source.url = f"{tmp_path.name}.xlsx"
    el_config_i.target.url = "pandas://"
    if source:
        el_config_i.source = Source.model_validate(
            el_config_i.source.model_dump(exclude_none=True) | source
        )
    execute(el_config_i)


def test_sheet(tmp_path):
    df1_o = pd.DataFrame({"a": [1, 2, 3]})

    push(tmp_path, {"df1": df1_o})

    pull(tmp_path)
    df1_i = staged_frames["df1"]
    assert len(df1_i) == 3
    assert df1_o.dtypes.equals(df1_i.dtypes)
    assert df1_o.equals(df1_i)


def test_sheets(tmp_path):
    df1_o = pd.DataFrame({"a": [1, 2, 3]})
    df2_o = pd.DataFrame({"b": [4, 5, 6]})

    push(tmp_path, {"df1": df1_o, "df2": df2_o})

    pull(tmp_path)
    df1_i = staged_frames["df1"]
    df2_i = staged_frames["df2"]
    assert len(df1_i) == 3
    assert len(df2_i) == 3
    assert df1_o.dtypes.equals(df1_i.dtypes)
    assert df2_o.dtypes.equals(df2_i.dtypes)
    assert df1_o.equals(df1_i)
    assert df2_o.equals(df2_i)


def test_sheets_single_target(tmp_path):
    df1_o = pd.DataFrame({"a": [1, 2, 3]})
    df2_o = pd.DataFrame({"a": [10, 20, 30]})
    df_o = pd.DataFrame({"a": [1, 2, 3, 10, 20, 30]})

    push(tmp_path, {"df1": df1_o, "df2": df2_o}, {"table": "df"})

    pull(tmp_path)
    df_i = staged_frames["df"]
    assert len(df_i) == 6
    assert df_o.dtypes.equals(df_i.dtypes)
    assert df_o.equals(df_i)


def test_sheet_skiprows(tmp_path):
    df1_o = pd.DataFrame({"a": [1, 2, 3]})

    push(tmp_path, {"df1": df1_o}, {"to_excel": {"startrow": 2}})

    pull(tmp_path)
    df1_i = staged_frames["df1"]
    assert len(df1_i) == 5

    pull(tmp_path, {"read_excel": {"skiprows": 2}})
    df1_i = staged_frames["df1"]
    assert len(df1_i) == 3
    assert df1_o.dtypes.equals(df1_i.dtypes)
    assert df1_o.equals(df1_i)


def test_sheet_skipfooter(tmp_path):
    df1_o = pd.DataFrame({"a": [1, 2, 3]})
    df1_f = pd.DataFrame({"a": [1, 2, 3, None, None, "Footer"]})

    push(tmp_path, {"df1": df1_f})

    pull(tmp_path)
    df1_i = staged_frames["df1"]
    assert len(df1_i) == 6

    pull(tmp_path, {"read_excel": {"skipfooter": 3}})
    df1_i = staged_frames["df1"]
    assert len(df1_i) == 3
    assert df1_o.dtypes.equals(df1_i.dtypes)
    assert df1_o.equals(df1_i)


def test_append_fixed(tmp_path):
    df1a_o = pd.DataFrame({"a": [1, 2, 3]})
    df1b_o = pd.DataFrame({"a": [10, 20, 30]})
    df1_o = pd.DataFrame({"a": [1, 2, 3, 10, 20, 30]})

    push(tmp_path, {"df": df1a_o})
    push(tmp_path, {"df": df1b_o}, {"if_exists": "append"})

    pull(tmp_path)
    df1_i = staged_frames["df"]
    assert len(df1_i) == 6
    assert df1_o.dtypes.equals(df1_i.dtypes)
    assert df1_o.equals(df1_i)


def test_append_mixed(tmp_path):
    # os.chdir(tmp_path)
    df1a_o = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [4, 5, 6],
        }
    )
    df1b_o = pd.DataFrame(
        {
            "b": [40, 50, 60],
            "a": [10, 20, 30],
        }
    )
    df1_o = pd.DataFrame(
        {
            "a": [1, 2, 3, 10, 20, 30],
            "b": [4, 5, 6, 40, 50, 60],
        }
    )

    push(tmp_path, {"df": df1a_o})
    push(tmp_path, {"df": df1b_o}, {"if_exists": "append"})

    pull(tmp_path)
    df1_i = staged_frames["df"]
    assert len(df1_i) == 6
    assert df1_o.dtypes.equals(df1_i.dtypes)
    assert df1_o.equals(df1_i)


def test_truncate(tmp_path):
    df1a_o = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [4, 5, 6],
        }
    )
    df1b_o = pd.DataFrame(
        {
            "b": [40, 50, 60],
            "a": [10, 20, 30],
        }
    )
    df1_o = pd.DataFrame(
        {
            "a": [10, 20, 30],
            "b": [40, 50, 60],
        }
    )

    push(tmp_path, {"df": df1a_o})
    push(tmp_path, {"df": df1b_o}, {"if_exists": "truncate"})

    pull(tmp_path)
    df1_i = staged_frames["df"]
    assert len(df1_i) == 3
    assert df1_o.dtypes.equals(df1_i.dtypes)
    assert df1_o.equals(df1_i)


def test_truncate_under(tmp_path):
    df1a_o = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [4, 5, 6],
        }
    )
    df1b_o = pd.DataFrame(
        {
            "b": [40, 50],
            "a": [10, 20],
        }
    )
    df1_o = pd.DataFrame(
        {
            "a": [10, 20],
            "b": [40, 50],
        }
    )

    push(tmp_path, {"df": df1a_o})
    push(tmp_path, {"df": df1b_o}, {"if_exists": "truncate"})

    pull(tmp_path)
    df1_i = staged_frames["df"]
    assert len(df1_i) == 2
    assert df1_o.dtypes.equals(df1_i.dtypes)
    assert df1_o.equals(df1_i)


def test_truncate_over(tmp_path):
    df1a_o = pd.DataFrame(
        {
            "a": [1, 2],
            "b": [4, 5],
        }
    )
    df1b_o = pd.DataFrame(
        {
            "b": [40, 50, 60],
            "a": [10, 20, 30],
        }
    )
    df1_o = pd.DataFrame(
        {
            "a": [10, 20, 30],
            "b": [40, 50, 60],
        }
    )

    push(tmp_path, {"df": df1a_o})
    push(tmp_path, {"df": df1b_o}, {"if_exists": "truncate"})

    pull(tmp_path)
    df1_i = staged_frames["df"]
    assert len(df1_i) == 3
    assert df1_o.dtypes.equals(df1_i.dtypes)
    assert df1_o.equals(df1_i)


def test_append_plus(tmp_path):
    df1a_o = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [4, 5, 6],
        }
    )
    df1b_o = pd.DataFrame(
        {
            "b": [40, 50, 60],
            "a": [10, 20, 30],
            "c": [70, 80, 90],
        }
    )
    df1_o = pd.DataFrame(
        {
            "a": [1, 2, 3, 10, 20, 30],
            "b": [4, 5, 6, 40, 50, 60],
        }
    )

    push(tmp_path, {"df": df1a_o})
    push(tmp_path, {"df": df1b_o}, {"if_exists": "append", "consistency": "ignore"})

    pull(tmp_path)
    df1_i = staged_frames["df"]
    assert len(df1_i) == 6
    assert df1_o.dtypes.equals(df1_i.dtypes)
    assert df1_o.equals(df1_i)


def test_append_minus(tmp_path):
    # adding Nones to coerce datatypes to floats
    df1a_o = pd.DataFrame(
        {
            "a": [1, 2, None, 3],
            "b": [4, 5, None, 6],
        }
    )
    df1b_o = pd.DataFrame(
        {
            "b": [40, 50, 60],
        }
    )
    df1_o = pd.DataFrame(
        {
            "a": [1, 2, None, 3, 40, 50, 60],
            "b": [4, 5, None, 6, None, None, None],
        }
    )

    push(tmp_path, {"df": df1a_o})
    push(tmp_path, {"df": df1b_o}, {"if_exists": "append", "consistency": "ignore"})

    pull(tmp_path)
    df1_i = staged_frames["df"]
    assert len(df1_i) == 7
    assert df1_o.dtypes.equals(df1_i.dtypes)
    assert df1_o.equals(df1_i)


def test_replace_sheet(tmp_path):
    df1_o = pd.DataFrame({"a": [1, 2, 3]})
    df2a_o = pd.DataFrame({"b": [4, 5, 6]})
    df2b_o = pd.DataFrame({"c": [7, 8, 9]})

    push(tmp_path, {"df1": df1_o, "df2": df2a_o})

    pull(tmp_path)
    df1_i = staged_frames["df1"]
    df2_i = staged_frames["df2"]
    assert len(df1_i) == 3
    assert len(df2_i) == 3
    assert df1_o.dtypes.equals(df1_i.dtypes)
    assert df2a_o.dtypes.equals(df2_i.dtypes)
    assert df1_o.equals(df1_i)
    assert df2a_o.equals(df2_i)

    push(tmp_path, {"df2": df2b_o}, {"if_exists": "replace"})

    pull(tmp_path)
    assert (len(staged_frames)) == 2
    df1_i = staged_frames["df1"]
    df2_i = staged_frames["df2"]
    assert len(df1_i) == 3
    assert len(df2_i) == 3
    assert df1_o.dtypes.equals(df1_i.dtypes)
    assert df2b_o.dtypes.equals(df2_i.dtypes)
    assert df1_o.equals(df1_i)
    assert df2b_o.equals(df2_i)


def test_replace_file(tmp_path):
    # os.chdir(tmp_path)
    df1_o = pd.DataFrame({"a": [1, 2, 3]})
    df2_o = pd.DataFrame({"b": [4, 5, 6]})

    push(tmp_path, {"df1": df1_o})
    push(tmp_path, {"df2": df2_o}, {"if_exists": "replace_file"})

    pull(tmp_path)
    df1_i = staged_frames["df2"]
    assert len(staged_frames) == 1
    assert len(df1_i) == 3
    assert df2_o.dtypes.equals(df1_i.dtypes)
    assert df2_o.equals(df1_i)
