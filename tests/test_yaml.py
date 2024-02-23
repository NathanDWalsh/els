import pandas as pd
import collections
import pytest
import yaml
import numpy as np

import os

import eel.config as ec

from eel.cli import execute
from eel.execute import pandas_end_points
from eel.path import get_config_default

Test = collections.namedtuple("Test", ["name", "df", "kwargs"])


@pytest.fixture(autouse=True, scope="session")
def setup():
    os.chdir("D:\\Sync\\repos\\eel\\temp")
    # os.delete("*.csv")
    yield


def test_cwd():
    assert "D:\\Sync\\repos\\eel\\temp" == os.getcwd()


def build_default_config() -> ec.Config:
    return ec.Config()


def is_numeric(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def get_df_config(df: pd.DataFrame) -> dict:
    config_df = get_config_default()
    if config_df.source.read_csv is None:
        config_df.source.read_csv = ec.ReadCsv()
    config_df.source.read_csv.dtype = df.dtypes.apply(lambda x: x.name).to_dict()

    # this is to prevent empty strings from being read as NaN:
    # config_df.source.read_csv.keep_default_na = False
    # config_df.source.read_csv.na_values = ["nan"]
    # = ["NaN", "Null", "null", "#NA", "N/A", "#N/A"]

    # config_df.source.read_csv.na_values = None

    config_dict = config_df.model_dump(exclude_none=True)

    return config_dict


@pytest.mark.parametrize(
    "dtype, expected", [(int, np.int32), (float, np.float64), (bool, np.bool_)]
)
def test_np_type_equality(dtype, expected):
    assert np.dtype(dtype) == expected


@pytest.mark.parametrize("py_val, expected", [(1, int), (1.1, float), (True, bool)])
def test_np_type_equality2(py_val, expected):
    assert np.array(py_val).dtype == expected


def get_atomic_strings():
    atomic_strings = {
        "Stringy": "a",
        "Inty": "1",
        "Floaty": "1.1",
        "Booly": "True",
        "Datey": "2021-01-01",
        "Datetimey": "2021-01-01 00:00:00",
        # "Empty": "",
        # "Noney": "None",
        # "Nully": "null",
        # "Nany": "NaN",
    }

    return atomic_strings


def get_atomic_numbers():
    atomic_numbers = {
        f"{num_type.__name__.capitalize()}({num_val})": np.array(num_val)
        .astype(np.dtype(num_type))
        .item()
        for num_type in [int, np.int32, np.int64, bool, np.bool_, float, np.float64]
        for num_val in [-1, 0, 1]
    }

    return atomic_numbers


def get_atomic_nans():
    atomic_NaNs = {
        f"NaN({num_type.__name__.capitalize()})": np.array(np.nan)
        .astype(np.dtype(num_type))
        .item()
        for num_type in [int, np.int32, np.int64, bool, np.bool_, float, np.float64]
    }

    return atomic_NaNs


def get_1r1c_tests(atomics: dict):

    test_frames = [
        Test(
            f"1r1c{type(val).__name__.capitalize()}{name}",
            pd.DataFrame({name: [val]}),
            {"quoting": quoting},
        )
        for name, val in atomics.items()
        for quoting in [0, 1, 2, 3]
        # consider adding 4 and 5 for later versions
        if not (
            quoting == 3
            and (val in ("", None) or np.isnan(val))
            # or ((not isinstance(val, str)) and np.isnan(val))
        )
    ]

    return test_frames


def get_atomic_number_tests():
    atomic_numbers = get_atomic_numbers()
    return get_1r1c_tests(atomic_numbers)


def get_atomic_nan_tests():
    atomic_nans = get_atomic_nans()
    return get_1r1c_tests(atomic_nans)


def id_func(testcase_vals):
    # if isinstance(testcase_vals, Test):
    return "_".join(
        (
            f"{name if not (name == 'name' or isinstance(value,dict) ) else ''}"
            f"{value if not isinstance(value,dict) else '_'.join( (f'{k}{v}') for k,v in value.items())}"
        )
        for name, value in testcase_vals._asdict().items()
        if not isinstance(value, pd.DataFrame)
    )
    # return ""


@pytest.mark.parametrize("test_case", get_atomic_nan_tests(), ids=id_func)
def test_csv_nan(test_case: Test, request):
    round_trip_csv(test_case, request)


def round_trip_csv(test_case: Test, request):
    # Access the fields of the Test named tuple using dot notation
    test_name = request.node.callspec.id
    df = test_case.df
    kwargs = test_case.kwargs

    test_file = test_name + ".csv"
    df.to_csv(test_file, index=False, **kwargs)

    df_config = get_df_config(df)
    test_eel = test_file + ".eel.yml"
    yaml.dump(
        df_config,
        open(test_eel, "w"),
        sort_keys=False,
        allow_unicode=True,
    )
    execute()
    df2 = pandas_end_points[test_name]

    assert df.equals(df2)

    os.remove(test_eel)
    os.remove(test_file)


def round_trip_excel(test_name: str, df: pd.DataFrame):
    df.to_excel("test.xlsx", index=False)
    df2 = pd.read_excel("test.xlsx")
    assert df.equals(df2)
    # os.remove("test.xlsx")
