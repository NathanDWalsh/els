import os

import pytest

from . import test_template as tt


# these tests are considered "symmetrical config", meaning the same config can be applied on
# either the push or pull operations with the same results
@pytest.mark.parametrize(
    "config_for",
    [
        "push",
        "pull",
    ],
)
@pytest.mark.parametrize(
    "test_name,flight_url",
    [
        ("pandas", tt.flight_url_df_dict),
        ("excel", tt.flight_url_excel),
        # ("sqlite", tt.flight_url_sqlite),
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
def test_symconfig(tmp_path, test_name, flight_url, func, config_for):
    os.chdir(tmp_path)
    func(flight_url=flight_url, config_for=config_for)


@pytest.mark.parametrize(
    "test_name,flight_url",
    [
        ("pandas", tt.flight_url_df_dict),
        ("excel", tt.flight_url_excel),
    ],
)
@pytest.mark.parametrize(
    "func",
    [
        tt.astype,
        tt.stack_dynamic,
    ],
)
def test_for_push_or_pull(tmp_path, test_name, flight_url, func):
    os.chdir(tmp_path)
    func(flight_url=flight_url)


@pytest.mark.parametrize(
    "test_name,flight_url",
    [
        ("excel", tt.flight_url_excel),
    ],
)
@pytest.mark.parametrize(
    "func",
    [
        tt.xl_multiindex_column,
        tt.xl_replace_file,
    ],
)
def test_for_excel(tmp_path, test_name, flight_url, func):
    os.chdir(tmp_path)
    func(flight_url=flight_url)
