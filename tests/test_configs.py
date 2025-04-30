import os

import pytest

from . import test_template as tt


# below tests are "symmetrical config": the same config can be applied
# on either the push or pull operations expecting the same results
@pytest.mark.parametrize(
    "config_for",
    [
        "push",
        "pull",
    ],
)
@pytest.mark.parametrize(
    "test_name",
    [
        ("pandas"),
        ("excel"),
        ("sqlite"),
        ("duckdb"),
        # ("mssql"),
        ("csv"),
        ("xml"),
    ],
)
@pytest.mark.parametrize(
    "func",
    [
        tt.single,
        tt.double_together,
        tt.double_together2,
        tt.double_separate,
        tt.append_together,
        tt.append_separate,
        tt.append_mixed,
        tt.append_minus,
        tt.split_on_col_explicit_tab,
        tt.filter,
        tt.prql,
        tt.prql_split,
        tt.add_columns,
        tt.pivot,
        tt.prql_split_pivot,
        tt.prql_col_split_pivot,
        tt.melt,
        tt.replace,
        tt.prql_col_split,
        tt.truncate_single,
        tt.truncate_double,
        tt.append_plus,
    ],
)
def test_sc(tmp_path, test_name, func, config_for):
    os.chdir(tmp_path)
    func(test_medium=test_name, config_for=config_for)


@pytest.mark.parametrize(
    "test_name",
    [
        ("pandas"),
        ("excel"),
        ("sqlite"),
        ("duckdb"),
        # ("mssql"),
        ("csv"),
    ],
)
@pytest.mark.parametrize(
    "func",
    [
        tt.astype,
        tt.stack_dynamic,
    ],
)
def test_for_push_or_pull(tmp_path, test_name, func):
    os.chdir(tmp_path)
    func(test_medium=test_name)


@pytest.mark.parametrize(
    "test_name",
    [
        ("excel"),
    ],
)
@pytest.mark.parametrize(
    "func",
    [
        tt.skiprows_xl1,
        tt.skiprows_xl2,
        tt.skiprows_xl3,
        tt.skipfoot_xl1,
        tt.skipfoot_xl2,
    ],
)
def test_for_push_and_pull_xl(tmp_path, test_name, func):
    os.chdir(tmp_path)
    func(test_medium=test_name)


@pytest.mark.parametrize(
    "test_name",
    [
        ("csv"),
    ],
)
@pytest.mark.parametrize(
    "func",
    [
        tt.skiprows_csv1,
        tt.skiprows_csv2,
        tt.skiprows_csv3,
        tt.skipfoot_csv1,
        tt.skipfoot_csv2,
    ],
)
def test_for_push_and_pull_csv(tmp_path, test_name, func):
    os.chdir(tmp_path)
    func(test_medium=test_name)


@pytest.mark.parametrize(
    "test_name",
    [
        ("excel"),
        ("csv"),
    ],
)
@pytest.mark.parametrize(
    "func",
    [
        tt.multiindex_column,
        tt.replace_file,
    ],
)
def test_for_files(tmp_path, test_name, func):
    os.chdir(tmp_path)
    func(test_medium=test_name)
