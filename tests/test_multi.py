import os

import pandas as pd

from els.cli import execute
from els.config import TargetIfExistsValue
from els.core import staged_frames
from els.path import get_config_default


def test_excel_sheet(tmp_path):
    os.chdir(tmp_path)
    df1_o = pd.DataFrame({"a": [1, 2, 3]})
    staged_frames.clear()
    staged_frames["df1"] = df1_o
    assert len(df1_o) == 3
    el_config_o = get_config_default()
    el_config_o.source.url = "pandas://"
    el_config_o.target.url = "single_excel_sheet.xlsx"
    execute(el_config_o)
    staged_frames.clear()
    el_config_i = get_config_default()
    el_config_i.target.url = "pandas://"
    el_config_i.source.url = el_config_o.target.url
    execute(el_config_i)
    df1_i = staged_frames["df1"]
    assert len(df1_i) == 3
    assert df1_o.dtypes.equals(df1_i.dtypes)
    assert df1_o.equals(df1_i)


def test_excel_replace(tmp_path):
    os.chdir(tmp_path)
    df1_o = pd.DataFrame({"a": [1, 2, 3]})
    df2_o = pd.DataFrame({"b": [4, 5, 6]})
    staged_frames.clear()
    staged_frames["df1"] = df1_o
    assert len(df1_o) == 3
    assert len(df2_o) == 3
    el_config_o = get_config_default()
    el_config_o.source.url = "pandas://"
    el_config_o.target.url = "replace_excel_sheet.xlsx"
    execute(el_config_o)

    staged_frames.clear()
    el_config_o.target.if_exists = TargetIfExistsValue.REPLACE_FILE
    staged_frames["df2"] = df2_o
    execute(el_config_o)

    staged_frames.clear()
    el_config_i = get_config_default()
    el_config_i.target.url = "pandas://"
    el_config_i.source.url = el_config_o.target.url
    execute(el_config_i)

    df1_i = staged_frames["df2"]
    assert "df1" not in staged_frames
    assert len(df1_i) == 3
    assert df2_o.dtypes.equals(df1_i.dtypes)
    assert df2_o.equals(df1_i)


def test_excel_sheets(tmp_path):
    os.chdir(tmp_path)
    df1_o = pd.DataFrame({"a": [1, 2, 3]})
    df2_o = pd.DataFrame({"b": [4, 5, 6]})
    assert len(df1_o) == 3
    assert len(df2_o) == 3
    staged_frames.clear()
    staged_frames["df1"] = df1_o
    staged_frames["df2"] = df2_o
    el_config_o = get_config_default()
    el_config_o.source.url = "pandas://"
    el_config_o.target.url = "multi_excel_sheets.xlsx"
    execute(el_config_o)
    staged_frames.clear()

    el_config_i = get_config_default()
    el_config_i.target.url = "pandas://"
    el_config_i.source.url = el_config_o.target.url
    execute(el_config_i)
    df1_i = staged_frames["df1"]
    df2_i = staged_frames["df2"]
    assert len(df1_i) == 3
    assert len(df2_i) == 3
    assert df1_o.dtypes.equals(df1_i.dtypes)
    assert df2_o.dtypes.equals(df2_i.dtypes)
    assert df1_o.equals(df1_i)
    assert df2_o.equals(df2_i)
