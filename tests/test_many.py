import pytest

from . import templates as tt
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
        tt.single,
        tt.double_together,
        tt.double_separate,
        tt.append_together,
        tt.append_separate,
        tt.append_mixed,
        tt.append_plus,
        tt.append_minus,
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
        tt.split_on_column_explicit_table,
        tt.filter,
        tt.prql,
        tt.prql_then_split,
        tt.add_columns,
        tt.pivot,
        tt.prql_then_split_then_pivot,
        tt.prql_col_then_split_then_pivot,
        tt.astype,
        tt.melt,
        tt.stack_dynamic,
        tt.truncate_single,
        tt.truncate_double,
        tt.replace,
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
        tt.prql_col_then_split,
    ],
)
def test_for_push_pull2(tmp_path, test_name, push, pull, func):
    func(push=push, pull=pull, tmp_path=tmp_path)
