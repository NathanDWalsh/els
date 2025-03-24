import pytest

from . import helpers as th
from . import test_excel as tx
from . import test_pandas as tp


@pytest.mark.parametrize(
    "test_name,test_args",
    [
        ("test_pandas", tp.push),
        ("test_excel", [tx.push, tx.pull]),
    ],
)
@pytest.mark.parametrize(
    "func",
    [
        th.single,
        th.double_together,
        th.double_separate,
        th.append_together,
        th.append_separate,
        th.append_mixed,
        th.append_plus,
        th.append_minus,
    ],
)
def test_for_callings(tmp_path, test_name, test_args, func):
    print(test_name)
    func(for_calling=test_args, tmp_path=tmp_path)


@pytest.mark.parametrize(
    "test_name,push,pull",
    [
        ("test_pandas", tp.push, None),
        ("test_excel", tx.push, tx.pull),
    ],
)
@pytest.mark.parametrize(
    "func",
    [
        th.split_on_column_explicit_table,
        th.filter,
        th.prql,
        th.prql_then_split,
        th.add_columns,
        th.pivot,
        th.prql_then_split_then_pivot,
        th.prql_col_then_split_then_pivot,
        th.astype,
        th.melt,
        th.stack_dynamic,
        th.truncate_single,
        th.truncate_double,
        th.replace,
    ],
)
def test_for_push_pull(tmp_path, test_name, push, pull, func):
    func(push=push, pull=pull, tmp_path=tmp_path)


@pytest.mark.parametrize(
    "test_name,push,pull",
    [
        ("test_pandas", tp.push, None),
        ("test_excel", tx.push, tx.pull),
    ],
)
@pytest.mark.parametrize(
    "func",
    [
        th.prql_col_then_split,
    ],
)
def test_for_push_pull2(tmp_path, test_name, push, pull, func):
    func(push=push, pull=pull, tmp_path=tmp_path)
