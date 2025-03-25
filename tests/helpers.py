import pandas as pd

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


def parse_func_and_kwargs(for_calling, global_kwargs: dict):
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
