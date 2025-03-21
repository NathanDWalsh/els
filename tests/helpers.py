import pandas as pd

import els.config as ec

outbound = {}
inbound = {}


def assert_dfs_equal(df0: pd.DataFrame, df1: pd.DataFrame):
    assert len(df0) == len(df1)
    assert len(df0.columns) == len(df1.columns)
    if not df0.dtypes.equals(df1.dtypes):
        raise Exception(f"types not equal: {[df0.dtypes], [df1.dtypes]}")
    assert df0.dtypes.equals(df1.dtypes)
    assert df0.columns.equals(df1.columns)
    assert df0.index.equals(df1.index)
    assert df0.equals(df1)


def assert_expected(expected, actual=None):
    if actual is None and inbound:
        actual = inbound
    assert id(expected) != id(actual)
    assert len(expected) > 0
    for k in expected.keys():
        if k not in actual:
            raise Exception([expected, actual])
        assert k in actual
        assert_dfs_equal(expected[k], actual[k])
    outbound.clear()


def to_list(x):
    if not isinstance(x, list):
        x = [x]
    return x


def to_call_list(for_calling):
    for_calling = to_list(for_calling)
    res = []
    for i in for_calling:
        if isinstance(i, tuple):
            res.append(i)
        elif i:
            res.append((i, {}))
    return res


def parse_func_and_kwargs(for_calling, global_kwargs):
    for_calling = to_call_list(for_calling)

    res = []

    push_kwargs = {}
    pull_kwargs = {}
    for k, v in global_kwargs.items():
        if k == "target":
            push_kwargs[k] = v
        elif v == "source":
            pull_kwargs[k] = v
        else:
            push_kwargs[k] = v
            pull_kwargs[k] = v

    for func in for_calling:
        if func[0].__name__ == "push":
            res.append((func[0], func[1] | push_kwargs))
        elif func[0].__name__ == "pull":
            res.append((func[0], func[1] | pull_kwargs))
        else:
            raise Exception(f"function not supported {func}")

    return res


def call_io_funcs(for_calling, **kwargs):
    for_calling = parse_func_and_kwargs(for_calling, kwargs)

    for func in for_calling:
        func[0](**func[1])


def clear_runways():
    global inbound
    inbound = {}
    global outbound
    outbound = {}


def single(for_calling, tmp_path=None):
    clear_runways()
    outbound["df"] = pd.DataFrame({"a": [1, 2, 3]})
    expected = outbound.copy()

    call_io_funcs(for_calling, **dict(tmp_path=tmp_path))
    assert_expected(expected)


def double_together(for_calling, tmp_path=None):
    clear_runways()
    outbound["dfa"] = pd.DataFrame({"a": [1, 2, 3]})
    outbound["dfb"] = pd.DataFrame({"b": [4, 5, 6]})
    expected = outbound.copy()

    call_io_funcs(for_calling, **dict(tmp_path=tmp_path))
    assert_expected(expected)


def double_separate(for_calling, tmp_path=None):
    clear_runways()
    outbound["dfa"] = pd.DataFrame({"a": [1, 2, 3]})
    call_io_funcs(for_calling, **dict(tmp_path=tmp_path))

    outbound.clear()
    outbound["dfb"] = pd.DataFrame({"b": [4, 5, 6]})
    call_io_funcs(for_calling, **dict(tmp_path=tmp_path))

    expected = {}
    expected["dfa"] = pd.DataFrame({"a": [1, 2, 3]})
    expected["dfb"] = pd.DataFrame({"b": [4, 5, 6]})

    assert_expected(expected)


def append_together(for_calling, tmp_path=None):
    clear_runways()
    outbound["df0a"] = pd.DataFrame({"a": [1, 2, 3]})
    outbound["df0b"] = pd.DataFrame({"a": [10, 20, 30]})
    expected = {}
    expected["df"] = pd.DataFrame({"a": [1, 2, 3, 10, 20, 30]})

    target = {"table": "df", "if_exists": "append"}
    call_io_funcs(for_calling, **dict(tmp_path=tmp_path, target=target))
    assert_expected(expected)


def append_separate(for_calling, tmp_path=None):
    clear_runways()
    outbound["df"] = pd.DataFrame({"a": [1, 2, 3]})
    call_io_funcs(for_calling, **dict(tmp_path=tmp_path))

    outbound.clear()
    outbound["df"] = pd.DataFrame({"a": [10, 20, 30]})
    expected = {}
    expected["df"] = pd.DataFrame({"a": [1, 2, 3, 10, 20, 30]})

    target = {"if_exists": "append"}
    call_io_funcs(for_calling, **dict(tmp_path=tmp_path, target=target))
    assert_expected(expected)


def append_mixed(for_calling, tmp_path=None):
    clear_runways()
    outbound["dfa"] = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [4, 5, 6],
        }
    )
    outbound["dfb"] = pd.DataFrame(
        {
            "b": [40, 50, 60],
            "a": [10, 20, 30],
        }
    )
    expected = {}
    expected["df"] = pd.DataFrame(
        {
            "a": [1, 2, 3, 10, 20, 30],
            "b": [4, 5, 6, 40, 50, 60],
        }
    )

    target = {"if_exists": "append", "table": "df"}
    call_io_funcs(for_calling, **dict(tmp_path=tmp_path, target=target))
    assert_expected(expected)


def append_plus(for_calling, tmp_path=None):
    clear_runways()
    outbound["dfa"] = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [4, 5, 6],
        }
    )
    outbound["dfb"] = pd.DataFrame(
        {
            "b": [40, 50, 60],
            "a": [10, 20, 30],
            "c": [70, 80, 90],
        }
    )
    expected = {}
    expected["df"] = pd.DataFrame(
        {
            "a": [1, 2, 3, 10, 20, 30],
            "b": [4, 5, 6, 40, 50, 60],
        }
    )

    target = {"if_exists": "append", "table": "df", "consistency": "ignore"}
    call_io_funcs(for_calling, **dict(tmp_path=tmp_path, target=target))
    assert_expected(expected)


def append_minus(for_calling, tmp_path=None):
    clear_runways()
    # adding Nones to coerce datatypes to floats
    outbound["dfa"] = pd.DataFrame(
        {
            "a": [1, 2, None, 3],
            "b": [4, 5, None, 6],
        }
    )
    outbound["dfb"] = pd.DataFrame(
        {
            "b": [40, 50, 60],
        }
    )
    expected = {}
    expected["df"] = pd.DataFrame(
        {
            "a": [1, 2, None, 3, None, None, None],
            "b": [4, 5, None, 6, 40, 50, 60],
        }
    )

    target = {"if_exists": "append", "table": "df", "consistency": "ignore"}
    call_io_funcs(for_calling, **dict(tmp_path=tmp_path, target=target))
    assert_expected(expected)


def truncate_single(push, pull=None, tmp_path=None):
    clear_runways()
    outbound["df"] = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [4, 5, 6],
        }
    )
    call_io_funcs(push, **dict(tmp_path=tmp_path))
    outbound.clear()

    outbound["df"] = pd.DataFrame(
        {
            "b": [30, 40],
            "a": [10, 20],
            "c": [50, 60],
        }
    )
    expected = {}
    expected["df"] = pd.DataFrame(
        {
            "a": [10, 20],
            "b": [30, 40],
        }
    )
    target = {"if_exists": "truncate", "consistency": "ignore"}
    call_io_funcs([push, pull], **dict(tmp_path=tmp_path, target=target))
    assert_expected(expected)


def truncate_double(push, pull=None, tmp_path=None):
    clear_runways()
    outbound["df"] = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [4, 5, 6],
        }
    )
    call_io_funcs(push, **dict(tmp_path=tmp_path))
    outbound.clear()

    outbound["dfa"] = pd.DataFrame(
        {
            "b": [50, 60],
            "a": [10, 20],
        }
    )
    outbound["dfb"] = pd.DataFrame(
        {
            "b": [70, 80],
        }
    )
    expected = {}
    expected["df"] = pd.DataFrame(
        {
            "a": [10, 20, None, None],
            "b": [50, 60, 70, 80],
        }
    )
    target = {"if_exists": "truncate", "table": "df", "consistency": "ignore"}
    call_io_funcs([push, pull], **dict(tmp_path=tmp_path, target=target))
    assert_expected(expected)


def replace(push, pull=None, tmp_path=None):
    clear_runways()
    outbound["a"] = pd.DataFrame({"a": [1, 2, 3]})
    outbound["b"] = pd.DataFrame({"b": [4, 5, 6]})
    expected = outbound.copy()
    push(tmp_path)

    if pull:
        pull(tmp_path)

    assert_expected(expected)

    outbound.clear()
    outbound["b"] = pd.DataFrame({"bb": [44, 55, 66]})
    expected.clear()
    expected["a"] = pd.DataFrame({"a": [1, 2, 3]})
    expected["b"] = pd.DataFrame({"bb": [44, 55, 66]})

    target = {"if_exists": "replace"}
    call_io_funcs([push], **dict(tmp_path=tmp_path, target=target))

    if pull:
        pull(tmp_path)
    assert_expected(expected)


# TODO: split_on_column_implicit_table


def split_on_column_explicit_table(push, pull=None, tmp_path=None):
    clear_runways()
    outbound["dfo"] = pd.DataFrame(
        {
            "split_col": ["t1", "t1", "t2", "t2"],
            "a": [1, 2, 3, 4],
            "b": [10, 20, 30, 40],
        }
    )
    source = {"split_on_column": "split_col", "table": "dfo"}
    # expected = outbound.copy()
    global inbound
    inbound = outbound

    push(tmp_path, source=source)

    if pull:
        pull(tmp_path)

    expected = {}
    expected["t1"] = pd.DataFrame(
        {
            "split_col": ["t1", "t1"],
            "a": [1, 2],
            "b": [10, 20],
        }
    )
    expected["t2"] = pd.DataFrame(
        {
            "split_col": ["t2", "t2"],
            "a": [3, 4],
            "b": [30, 40],
        }
    )

    assert_expected(expected)


def split_on_column_explicit_table2(push, pull=None, tmp_path=None):
    clear_runways()
    outbound["dfo"] = pd.DataFrame(
        {
            "split_col": ["t1", "t1", "t2", "t2"],
            "a": [1, 2, 3, 4],
            "b": [10, 20, 30, 40],
        }
    )
    transform = ec.SplitOnColumn(column_name="split_col")
    # transform.
    source = {"table": "dfo"}
    # expected = outbound.copy()
    global inbound
    inbound = outbound

    push(tmp_path, source=source, transform=transform)

    if pull:
        pull(tmp_path)

    expected = {}
    expected["t1"] = pd.DataFrame(
        {
            "split_col": ["t1", "t1"],
            "a": [1, 2],
            "b": [10, 20],
        }
    )
    expected["t2"] = pd.DataFrame(
        {
            "split_col": ["t2", "t2"],
            "a": [3, 4],
            "b": [30, 40],
        }
    )

    assert_expected(expected)
